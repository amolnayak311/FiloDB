package filodb.core.memstore


import filodb.core.binaryrecord2.{RecordSchema, StringifyMapItemConsumer}
import filodb.core.metadata.Column.ColumnType._

import com.typesafe.scalalogging.StrictLogging


trait TimeSeriesLifecycleListener extends StrictLogging {



  /**
   * Invoked when a timeseries is activated, the callback will be called in the thread processing the
   * ingestion and thus need to be return quickly and not block on expensive computation and IO
   *
   * @param partKeyBase  byte array for the partition key
   * @param partKeyOffset
   * @param partSchema
   *
   */
  def timeSeriesActivated(partKeyBase: Any, partKeyOffset: Long, partSchema: RecordSchema): Unit

  /**
   * Invoked when a timeseries is deactivated, the callback will be called in the thread processing the
   * ingestion and thus need to be return quickly and not block on expensive computation and IO
   *
   * @param partKeyBase  byte array for the partition key
   * @param partKeyOffset
   * @param partSchema
   */
  def timeSeriesDeactivated(partKeyBase: Any, partKeyOffset: Long, partSchema: RecordSchema): Unit



  def getLabelsFromPartitionKey(partKeyBase: Any, partKeyOffset: Long, partSchema: RecordSchema):
  Map[String, String]  = {
    Range(0, partSchema.numColumns).map{colNum: Int => {
        val col = partSchema.columns(colNum)
        col.colType match {
          case LongColumn => Map(col.name -> partSchema.getLong(partKeyBase, partKeyOffset, colNum).toString)
          case IntColumn => Map(col.name -> partSchema.getInt(partKeyBase, partKeyOffset, colNum).toString)
          case StringColumn => Map(col.name -> partSchema.asJavaString(partKeyBase, partKeyOffset, colNum))
          case DoubleColumn => Map(col.name -> partSchema.getDouble(partKeyBase, partKeyOffset, colNum).toString)
          case MapColumn => {
            val c = new StringifyMapItemConsumer();
            partSchema.consumeMapItems(partKeyBase, partKeyOffset, colNum, c);
            c.stringPairs.toMap[String, String]
          }
          case _ => {
            logger.warn("Type " + col.colType + " not supported, values will be lost with tracking cardinality")
            Map.empty[String, String]
          }
        }
      }
    }.reduce((m1, m2) => {
      m1 ++ m2
    })
  }


}