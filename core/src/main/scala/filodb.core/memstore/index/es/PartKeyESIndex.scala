package filodb.core.memstore.index.es

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Base64

import scala.collection.mutable.{Map => MutableMap}

import co.elastic.clients.elasticsearch._types.query_dsl.Query
import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch.core.SearchRequest
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.rest_client.RestClientTransport
import com.fasterxml.jackson.databind.ObjectMapper
import com.googlecode.javaewah.EWAHCompressedBitmap
import com.typesafe.scalalogging.StrictLogging
import org.apache.http.HttpHost
import org.elasticsearch.client.{Request, RestClient}
import spire.syntax.cfor.cforRange

import filodb.core._
import filodb.core.Types.PartitionKey
import filodb.core.binaryrecord2.MapItemConsumer
import filodb.core.memstore._
import filodb.core.metadata.Column.ColumnType.{MapColumn, StringColumn}
import filodb.core.metadata.PartitionSchema
import filodb.core.query.ColumnFilter
import filodb.memory.{UTF8StringMedium, UTF8StringShort}
import filodb.memory.format.{ZeroCopyUTF8String => UTF8Str}
import filodb.memory.format.UnsafeUtils


object PartKeyESIndex {
  final val PART_ID       = "__partId__"
  final val START_TIME    = "__startTime__"
  final val END_TIME      = "__endTime__"
  final val PART_KEY      = "__partKey__"
  final val MAX_STR_INTERN_ENTRIES = 10000
}

class PartKeyESIndex(ref: DatasetRef,
                     schema: PartitionSchema,
                     shardNum: Int,
                     retentionMillis: Long // only used to calculate fallback startTime
                    ) extends StrictLogging  with AutoCloseable{


  import PartKeyESIndex._

  // TODO: Hardcoded to local instance, change this
  val restClient: RestClient = RestClient.builder(
    new HttpHost("localhost", 9200, "http")).build

  val transport = new RestClientTransport(restClient, new JacksonJsonpMapper)

  val client = new ElasticsearchClient(transport)


  val om = new ObjectMapper();


  private val esDocument = new ThreadLocal[MutableMap[String, Any]]() {
    override def initialValue(): MutableMap[String, Any] = MutableMap.empty
  }

  private val utf8ToStrCache = concurrentCache[UTF8Str, String](MAX_STR_INTERN_ENTRIES)

  def bytesRefToUnsafeOffset(bytesRefOffset: Int): Int = bytesRefOffset + UnsafeUtils.arayOffset

  def unsafeOffsetToBytesRefOffset(offset: Long): Int = offset.toInt - UnsafeUtils.arayOffset

  private val numPartColumns = schema.columns.length
  private final val sha256Digest = MessageDigest.getInstance("SHA-256")

  private val mapConsumer = new MapItemConsumer {
    def consume(keyBase: Any, keyOffset: Long, valueBase: Any, valueOffset: Long, index: Int): Unit = {
      import filodb.core._
      val key = utf8ToStrCache.getOrElseUpdate(new UTF8Str(keyBase, keyOffset + 1,
        UTF8StringShort.numBytes(keyBase, keyOffset)),
        _.toString)
      val value = new String(valueBase.asInstanceOf[Array[Byte]],
        unsafeOffsetToBytesRefOffset(valueOffset + 2), // add 2 to move past numBytes
        UTF8StringMedium.numBytes(valueBase, valueOffset), StandardCharsets.UTF_8)
      esDocument.get += key -> value
    }
  }

  private final val indexers = schema.columns.zipWithIndex.map { case (c, pos) =>
    c.columnType match {
      case StringColumn => new Indexer {
        val colName = UTF8Str(c.name)
        def fromPartKey(base: Any, offset: Long, partIndex: Int): Unit = {
          val strOffset = schema.binSchema.blobOffset(base, offset, pos)
          val numBytes = schema.binSchema.blobNumBytes(base, offset, pos)
          val value = new String(base.asInstanceOf[Array[Byte]], strOffset.toInt - UnsafeUtils.arayOffset,
            numBytes, StandardCharsets.UTF_8)
          esDocument.get() += colName.toString -> value
        }
        def getNamesValues(key: PartitionKey): Seq[(UTF8Str, UTF8Str)] = ??? // not used
      }
      case MapColumn => new Indexer {
        def fromPartKey(base: Any, offset: Long, partIndex: Int): Unit = {
          schema.binSchema.consumeMapItems(base, offset, pos, mapConsumer)
        }
        def getNamesValues(key: PartitionKey): Seq[(UTF8Str, UTF8Str)] = ??? // not used
      }
      case other: Any =>
        logger.warn(s"Column $c has type that cannot be indexed and will be ignored right now")
        NoOpIndexer
    }
  }.toArray


  def reset(partId: Int, partKey: Array[Byte], startTime: Long, endTime: Long): MutableMap[String, Any] = {
    val map = esDocument.get()
    map.clear()
    map += (START_TIME -> startTime)
    map += (END_TIME -> endTime)
    map += (PART_ID -> partId)
    map += (PART_KEY -> new String(Base64.getEncoder().encode(partKey), StandardCharsets.ISO_8859_1))
    map
  }


  private def makeDocument(partKeyOnHeapBytes: Array[Byte],
                           partKeyBytesRefOffset: Int,
                           partKeyNumBytes: Int,
                           partId: Int, startTime: Long, endTime: Long): (String, String) = {
    val map = reset(partId, partKeyOnHeapBytes, startTime, endTime)
    cforRange { 0 until numPartColumns } { i =>
      indexers(i).fromPartKey(partKeyOnHeapBytes, bytesRefToUnsafeOffset(partKeyBytesRefOffset), partId)
    }

    val pkHash = Base64.getEncoder().encode(sha256Digest.digest(partKeyOnHeapBytes)).map("%02x" format _).mkString
    import scala.collection.JavaConverters.mapAsJavaMap
    (pkHash, om.writeValueAsString(mapAsJavaMap(map)))
  }

  def partIdsEndedBefore(endedBefore: Long): debox.Buffer[Int] = ???
  def removePartKeys(partIds: debox.Buffer[Int]): Unit = ???
  def indexValues(fieldName: String, topK: Int = 100): Seq[TermInfo] = ???
  def indexNames(limit: Int): Seq[String] = ???


  def addPartKey(partKeyOnHeapBytes: Array[Byte], partId: Int, startTime: Long,
                 endTime: Long = Long.MaxValue, partKeyBytesRefOffset: Int = 0)
                (partKeyNumBytes: Int = partKeyOnHeapBytes.length): Unit = {

    val (id, document) =
      makeDocument(partKeyOnHeapBytes, partKeyBytesRefOffset, partKeyNumBytes, partId, startTime, endTime)
    print(s"$id, $document")
    val req = new Request("PUT", s"/partkeys/_doc/$id");
    req.addParameter("routing", shardNum.toString)
    req.setJsonEntity(document)
    val resp = restClient.performRequest(req)
    logger.info(s"Indexed partKey with hash $id, index returned response ${resp.getStatusLine.getStatusCode}")
  }
  def upsertPartKey(partKeyOnHeapBytes: Array[Byte], partId: Int,
                    startTime: Long, endTime: Long = Long.MaxValue, partKeyBytesRefOffset: Int = 0)
                   (partKeyNumBytes: Int = partKeyOnHeapBytes.length): Unit = ???
  // LuceneAPI exposed
  //def partKeyFromPartId(partId: Int): Option[BytesRef]
  // def foreachPartKeyStillIngesting(func: (Int, BytesRef) => Unit):
  def startTimeFromPartId(partId: Int): Long = ???
  def startTimeFromPartIds(partIds: Iterator[Int]): debox.Map[Int, Long] = ???
  def endTimeFromPartId(partId: Int): Long = ???
  def partIdsOrderedByEndTime(topk: Int, fromEndTime: Long = 0,
                              toEndTime: Long = Long.MaxValue): EWAHCompressedBitmap = ???

  def updatePartKeyWithEndTime(partKeyOnHeapBytes: Array[Byte], partId: Int,
                               endTime: Long = Long.MaxValue, partKeyBytesRefOffset: Int = 0)
                              (partKeyNumBytes: Int = partKeyOnHeapBytes.length): Unit = ???

  private def buildSearchRequest(columnFilters: Seq[ColumnFilter], startTime: Long, endTime: Long): SearchRequest = {
    val builder = new SearchRequest.Builder()
    val queryBuilder = new Query.Builder()
    queryBuilder.bool()

    ???
  }

  def partIdsFromFilters(columnFilters: Seq[ColumnFilter], startTime: Long, endTime: Long): debox.Buffer[Int] = {

    ???
  }
  // Method name and signature does not make sense,
  def labelNamesFromFilters(columnFilters: Seq[ColumnFilter], startTime: Long, endTime: Long): Int = ???
  def partKeyRecordsFromFilters(columnFilters: Seq[ColumnFilter],
                                startTime: Long, endTime: Long): Seq[PartKeyLuceneIndexRecord] = ???
  def partIdFromPartKeySlow(partKeyBase: Any, partKeyOffset: Long): Option[Int] = ???


  def indexNumEntries: Long = ???

  def reset(): Unit = {
    // NOP for now
  }

  def close(): Unit = {
    restClient.close()
  }

  def refreshReadersBlocking(): Unit = {
    // Lucene specific, NOP for ES, kept to make the spec happy
  }

}
