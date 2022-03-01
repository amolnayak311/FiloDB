package filodb.core.memstore.index.es

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Base64

import scala.collection.mutable.{Map => MutableMap}

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch._types.{FieldSort, FieldValue, SortOptions, SortOrder}
import co.elastic.clients.elasticsearch._types.query_dsl._
import co.elastic.clients.elasticsearch.core.SearchRequest
import co.elastic.clients.elasticsearch.core.search.{Hit, SourceConfig, SourceFilter}
import co.elastic.clients.json.JsonData.of
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.rest_client.RestClientTransport
import com.fasterxml.jackson.databind.ObjectMapper
import com.googlecode.javaewah.EWAHCompressedBitmap
import com.typesafe.scalalogging.StrictLogging
import org.apache.http.HttpHost
import org.elasticsearch.client.{Cancellable, Request, ResponseListener, RestClient}
import spire.syntax.cfor.cforRange

import filodb.core._
import filodb.core.Types.PartitionKey
import filodb.core.binaryrecord2.MapItemConsumer
import filodb.core.memstore._
import filodb.core.metadata.Column.ColumnType.{MapColumn, StringColumn}
import filodb.core.metadata.PartitionSchema
import filodb.core.query.ColumnFilter
import filodb.core.query.Filter.{And, Equals, EqualsRegex, In, NotEquals, NotEqualsRegex}
import filodb.memory.{UTF8StringMedium, UTF8StringShort}
import filodb.memory.format.{ZeroCopyUTF8String => UTF8Str}
import filodb.memory.format.UnsafeUtils


object PartKeyESIndex {
  final val PART_ID       = "__partId__"
  final val START_TIME    = "__startTime__"
  final val END_TIME      = "__endTime__"
  final val PART_KEY      = "__partKey__"
  final val MAX_STR_INTERN_ENTRIES = 10000
  final val INDEX_NAME    = "partkeys"
  // This is the default number of response
  // TODO: Make this configurable, tweek index.max_result_window if needed or use scrollable API
  final val MAX_NUM_RESULTS = 10000
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
                           partId: Int, startTime: Long, endTime: Long) : String = {
    val map = reset(partId, partKeyOnHeapBytes, startTime, endTime)
    cforRange { 0 until numPartColumns } { i =>
      indexers(i).fromPartKey(partKeyOnHeapBytes, bytesRefToUnsafeOffset(partKeyBytesRefOffset), partId)
    }

    // Add SHA256 hash instead of partkey later
    // val pkHash = Base64.getEncoder().encode(sha256Digest.digest(partKeyOnHeapBytes)).map("%02x" format _).mkString
    import scala.collection.JavaConverters.mapAsJavaMap
    om.writeValueAsString(mapAsJavaMap(map))
  }

  def partIdsEndedBefore(endedBefore: Long): debox.Buffer[Int] = {
    val query = buildQuery(Nil, 0, endTime = endedBefore - 1)
    val response = client.search((s: SearchRequest.Builder) =>
      s.size(MAX_NUM_RESULTS)
        .index(INDEX_NAME)
        .source( (sc: SourceConfig.Builder) =>
          sc.filter(
            (filter: SourceFilter.Builder) => filter.includes(PART_ID)))
        .query(query), classOf[java.util.Map[String, Int]])

    debox.Buffer.fromIterable(response.hits().hits().stream.mapToInt(
      (h: Hit[java.util.Map[String, Int]]) => h.source().get(PART_ID).toString().toInt).toArray)

  }
  def removePartKeys(partIds: debox.Buffer[Int]): Unit = ???
  def indexValues(fieldName: String, topK: Int = 100): Seq[TermInfo] = ???
  def indexNames(limit: Int): Seq[String] = ???


  def addPartKeyAsync(partKeyOnHeapBytes: Array[Byte], partId: Int, startTime: Long,
                      endTime: Long = Long.MaxValue, partKeyBytesRefOffset: Int = 0)
                     (partKeyNumBytes: Int = partKeyOnHeapBytes.length)(listener: ResponseListener): Cancellable = {

    val document =
      makeDocument(partKeyOnHeapBytes, partKeyBytesRefOffset, partKeyNumBytes, partId, startTime, endTime)

    // Do not use id and use autogenerated ones to prevent expensive id check during insertion
    val req = new Request("POST", s"/$INDEX_NAME/_doc")
    req.addParameter("routing", shardNum.toString)
    req.setJsonEntity(document)
    restClient.performRequestAsync(req, listener)

  }
  def addPartKey(partKeyOnHeapBytes: Array[Byte], partId: Int, startTime: Long,
                 endTime: Long = Long.MaxValue, partKeyBytesRefOffset: Int = 0)
                (partKeyNumBytes: Int = partKeyOnHeapBytes.length): Unit = {
    val document =
      makeDocument(partKeyOnHeapBytes, partKeyBytesRefOffset, partKeyNumBytes, partId, startTime, endTime)

    // Do not use id and use autogenerated ones to prevent expensive id check during insertion
    // TODO: IMPORTANT: To get Testcases working, suffix id with partId. id is the hash of partKey. Is it valid to have
    //  multiple documents for same part key?
    val req = new Request("POST", s"/$INDEX_NAME/_doc")
    req.addParameter("routing", shardNum.toString)
    req.setJsonEntity(document)
    val resp = restClient.performRequest(req)
    logger.info(s"index addition returned response ${resp.getStatusLine.getStatusCode}")
  }
  def upsertPartKey(partKeyOnHeapBytes: Array[Byte], partId: Int,
                    startTime: Long, endTime: Long = Long.MaxValue, partKeyBytesRefOffset: Int = 0)
                   (partKeyNumBytes: Int = partKeyOnHeapBytes.length): Unit = ???
  // LuceneAPI exposed
  //def partKeyFromPartId(partId: Int): Option[BytesRef]
  // def foreachPartKeyStillIngesting(func: (Int, BytesRef) => Unit):
  def startTimeFromPartId(partId: Int): Long = ???
  def startTimeFromPartIds(partIds: Iterator[Int]): debox.Map[Int, Long] = {

    val query = buildQuery(Seq(ColumnFilter(PART_ID, In(partIds.toSet))), startTime = 0, endTime = 0)
//    val factory = new com.fasterxml.jackson.core.JsonFactory
//    val jsonObjectWriter = new java.io.StringWriter
//    val gen = new co.elastic.clients.json.jackson.JacksonJsonpGenerator(factory.createGenerator(jsonObjectWriter))
//    query.serialize(gen, new JacksonJsonpMapper)
//    gen.flush()
//    print(jsonObjectWriter)
    val response = client.search((s: SearchRequest.Builder) =>
      s.size(MAX_NUM_RESULTS)
        .index(INDEX_NAME)
        .source( (sc: SourceConfig.Builder) =>
          sc.filter(
            (filter: SourceFilter.Builder) => filter.includes(PART_ID, START_TIME)))
        .query(query), classOf[java.util.Map[String, _]])

    val res = debox.Map.empty[Int, Long]
    response.hits().hits().stream().forEach(
      (x : Hit[java.util.Map[String, _]])=>
        res.update(x.source().get(PART_ID).asInstanceOf[Int],
          x.source().get(START_TIME).asInstanceOf[Long].longValue()))
    res
  }
  def endTimeFromPartId(partId: Int): Long = ???
  def partIdsOrderedByEndTime(topk: Int, fromEndTime: Long = 0,
                              toEndTime: Long = Long.MaxValue): EWAHCompressedBitmap = {

    val query = buildQuery(Nil, fromEndTime, toEndTime)
    val response = client.search((s: SearchRequest.Builder) =>
      s.size(MAX_NUM_RESULTS.min(topk))
        .index(INDEX_NAME)
        .sort((so: SortOptions.Builder) => so.field(
          (fs: FieldSort.Builder) => fs.field(END_TIME).order(SortOrder.Asc)))
        .source( (sc: SourceConfig.Builder) =>
          sc.filter(
            (filter: SourceFilter.Builder) => filter.includes(PART_ID)))
        .query(query), classOf[java.util.Map[String, Int]])
    val res = new EWAHCompressedBitmap()
    response.hits().hits().stream().forEach(
      (x : Hit[java.util.Map[String, Int]])=> {
        res.set(x.source().get(END_TIME))
      })
    res
  }


  def updatePartKeyWithEndTime(partKeyOnHeapBytes: Array[Byte], partId: Int,
                               endTime: Long = Long.MaxValue, partKeyBytesRefOffset: Int = 0)
                              (partKeyNumBytes: Int = partKeyOnHeapBytes.length): Unit = {
    val updateEndDate = """{
      |  "script": {
      |    "lang": "painless",
      |    "source": "ctx._source.__endTime__ = params['endTime']",
      |    "params": {
      |       "endTime": %d
      |     }
      |  },
      |  "query": {
      |    "term": {
      |      "__partId__": {
      |        "value": "%d"
      |      }
      |    }
      |  },
      |  "max_docs" : 1
      |}""".stripMargin.format(endTime, partId)
    val req = new Request("POST", s"/$INDEX_NAME/_update_by_query")
    req.addParameter("routing", shardNum.toString)
    req.setJsonEntity(updateEndDate)
    val resp = restClient.performRequest(req)
    logger.info("updatePartKeyWithEndTime for partId=%d with endDate=%d returned statusCode=%d",
      partId, endTime, resp.getStatusLine.getStatusCode)

  }

  // scalastyle:off method.length
  private def buildQuery(columnFilters: Seq[ColumnFilter], startTime: Long, endTime: Long): Query = {
    val builder: Query.Builder = new Query.Builder
    builder.constantScore( (c: ConstantScoreQuery.Builder) => {
      c.filter((f: Query.Builder) => {
        f.bool( (bool: BoolQuery.Builder)  => {
            if (startTime > 0) {
              // Documents whose start date <= provided end date
              bool.must((m : Query.Builder) => {
                m.range((r: RangeQuery.Builder) => r.field(START_TIME).lte(of(endTime)))
              })
            }

            if (endTime > 0) {
              bool.must((m : Query.Builder) => {
                m.range((r: RangeQuery.Builder) => r.field(END_TIME).gte(of(startTime)))
              })
            }
            columnFilters.foreach { case ColumnFilter(column, filter) =>
              filter match {
                case EqualsRegex(value) =>
                  ???
                case NotEqualsRegex(value) =>
                  ???
                case Equals(value) =>
                  bool.must((f: Query.Builder) =>
                    f.term((t: TermQuery.Builder) => t.field(column).value(FieldValue.of(value.toString))))
                case NotEquals(value) =>
                  val strValue = value.toString
                  bool.mustNot((f: Query.Builder) =>
                    f.term((t: TermQuery.Builder) => t.field(column).value(FieldValue.of(strValue))))
                  if(strValue.isEmpty)
                    bool.filter(
                      (filter: Query.Builder) => filter.regexp(
                        (rb: RegexpQuery.Builder) => rb.field(column).value(".*"))
                    )
                  else
                    bool.filter((filter: Query.Builder) => filter.matchAll((ma: MatchAllQuery.Builder) => ma))
                case In(values) =>
                  bool.must((m: Query.Builder) => {
                    m.bool((bo: BoolQuery.Builder) => {
                      values.foreach(v => {
                        bo.should(
                          (f: Query.Builder) =>
                            f.term((t: TermQuery.Builder) => t.field(column).value(FieldValue.of(v.toString))))
                      })
                      bo
                    })
                  })

                case And(lhs, rhs) =>
                  ???
                case _ => throw new UnsupportedOperationException
              }
            }
            bool.must((m : Query.Builder) => {
              m.term((t: TermQuery.Builder) => t.field("_routing").value(FieldValue.of(shardNum)))
            })
        })
      })
    })
    builder.build()
  }
  // scalastyle:on method.length

  def partIdsFromFilters(columnFilters: Seq[ColumnFilter], startTime: Long, endTime: Long): debox.Buffer[Int] = {
    val query = buildQuery(columnFilters, startTime, endTime)

//    val factory = new com.fasterxml.jackson.core.JsonFactory
//    val jsonObjectWriter = new java.io.StringWriter
//    val gen = new co.elastic.clients.json.jackson.JacksonJsonpGenerator(factory.createGenerator(jsonObjectWriter))
//    query.serialize(gen, new JacksonJsonpMapper)
//    gen.flush()
//    print(jsonObjectWriter)

    val response = client.search((s: SearchRequest.Builder) =>
        s.size(MAX_NUM_RESULTS)
        .index(INDEX_NAME)
        .source( (sc: SourceConfig.Builder) =>
          sc.filter(
            (filter: SourceFilter.Builder) => filter.includes(PART_ID)))    // Just get PartIDs
      .query(query), classOf[java.util.Map[String, _]])


    debox.Buffer.fromIterable(response.hits().hits().stream.mapToInt(
      (h: Hit[java.util.Map[String, _]]) => h.source().get(PART_ID).toString().toInt).toArray)
  }
  // Method name and signature does not make sense,
  def labelNamesFromFilters(columnFilters: Seq[ColumnFilter], startTime: Long, endTime: Long): Int = ???
  def partKeyRecordsFromFilters(columnFilters: Seq[ColumnFilter],
                                startTime: Long, endTime: Long): Seq[PartKeyLuceneIndexRecord] = {
    val query = buildQuery(columnFilters, startTime, endTime)

    //    val factory = new com.fasterxml.jackson.core.JsonFactory
    //    val jsonObjectWriter = new java.io.StringWriter
    //    val gen = new co.elastic.clients.json.jackson.JacksonJsonpGenerator(factory.createGenerator(jsonObjectWriter))
    //    query.serialize(gen, new JacksonJsonpMapper)
    //    gen.flush()
    //    print(jsonObjectWriter)

    val response = client.search((s: SearchRequest.Builder) =>
      s.size(MAX_NUM_RESULTS)
        .index(INDEX_NAME)
        .source( (sc: SourceConfig.Builder) =>
          sc.filter(
            (filter: SourceFilter.Builder) => filter.includes(PART_KEY, START_TIME, END_TIME)))
        .query(query), classOf[java.util.Map[String, Any]])


    import scala.collection.JavaConverters.asScalaIteratorConverter;
    asScalaIteratorConverter(response.hits().hits().iterator()).asScala.toSeq.map( hit => {
      val map = hit.source()
      val partKeyBase64: String = map.getOrDefault(PART_KEY, "").asInstanceOf[String]
      val start = map.getOrDefault(START_TIME, -1L).asInstanceOf[Number].longValue()
      val end = map.getOrDefault(END_TIME, -1).asInstanceOf[Number].longValue()
      PartKeyLuceneIndexRecord(Base64.getDecoder.decode(partKeyBase64), start, end)
    })
  }
  def partIdFromPartKeySlow(partKeyBase: Any, partKeyOffset: Long): Option[Int] = ???


  def indexNumEntries: Long = ???

  def reset(): Unit = {
    // NOP for now
  }

  def close(): Unit = {
    restClient.close()
  }

  def refreshReadersBlocking(): Unit = {
    val req = new Request("POST", s"/$INDEX_NAME/_refresh")
    val resp = restClient.performRequest(req)
    logger.info("Got status code {} after refreshing index", resp.getStatusLine.getStatusCode);
  }

  //IMPORTANT: Only used in test cases
  private[es] def deleteAll(): Unit = {
    val query: Query = new Query.Builder().matchAll((v: MatchAllQuery.Builder) => v).build()
    val req = new Request("POST", s"/$INDEX_NAME/_delete_by_query?conflicts=proceed")
    req.addParameter("routing", shardNum.toString)
    req.setJsonEntity("""{
                        |  "query": {
                        |    "match_all": {}
                        |  }
                        |}""".stripMargin)
    val resp = restClient.performRequest(req)
    logger.info("Got status code {} after deleting all", resp.getStatusLine.getStatusCode);

  }

}
