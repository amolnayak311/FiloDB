package filodb.core.memstore.index.es

import com.googlecode.javaewah.EWAHCompressedBitmap
import com.typesafe.scalalogging.StrictLogging

import filodb.core.DatasetRef
import filodb.core.memstore.{PartKeyLuceneIndexRecord, TermInfo}
import filodb.core.metadata.PartitionSchema
import filodb.core.query.ColumnFilter

class PartKeyESIndex(ref: DatasetRef,
                     schema: PartitionSchema,
                     shardNum: Int,
                     retentionMillis: Long // only used to calculate fallback startTime
                    ) extends StrictLogging {


  def partIdsEndedBefore(endedBefore: Long): debox.Buffer[Int] = ???
  def removePartKeys(partIds: debox.Buffer[Int]): Unit = ???
  def indexValues(fieldName: String, topK: Int = 100): Seq[TermInfo] = ???
  def indexNames(limit: Int): Seq[String] = ???
  def addPartKey(partKeyOnHeapBytes: Array[Byte], partId: Int, startTime: Long,
                 endTime: Long = Long.MaxValue, partKeyBytesRefOffset: Int = 0)
                (partKeyNumBytes: Int = partKeyOnHeapBytes.length): Unit = ???
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

  def partIdsFromFilters(columnFilters: Seq[ColumnFilter], startTime: Long, endTime: Long): debox.Buffer[Int] = ???
  // Method name and signature does not make sense,
  def labelNamesFromFilters(columnFilters: Seq[ColumnFilter], startTime: Long, endTime: Long): Int = ???
  def partKeyRecordsFromFilters(columnFilters: Seq[ColumnFilter],
                                startTime: Long, endTime: Long): Seq[PartKeyLuceneIndexRecord] = ???
  def partIdFromPartKeySlow(partKeyBase: Any, partKeyOffset: Long): Option[Int] = ???


  def indexNumEntries: Long = ???

  def reset(): Unit = {
    // NOP for now
  }
  def refreshReadersBlocking(): Unit = {
    // Lucene specific, NOP for ES, kept to make the spec happy
  }

}
