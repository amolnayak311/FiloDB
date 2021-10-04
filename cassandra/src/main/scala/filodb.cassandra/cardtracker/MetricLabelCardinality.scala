package filodb.cassandra.cardtracker

import scala.concurrent.{ExecutionContext, Future}

import com.datastax.driver.core.ConsistencyLevel

import filodb.cassandra.FiloCassandraConnector
import filodb.cassandra.columnstore.BaseDatasetTable
import filodb.core.{DatasetRef, Response}

class MetricLabelCardinality(val dataset: DatasetRef,
                       val connector: FiloCassandraConnector,
                       writeConsistencyLevel: ConsistencyLevel)
                      (implicit ec: ExecutionContext)extends BaseDatasetTable {

  override def suffix: String = "metric_label_cardinality"

  private lazy val getMetricLabelsValueCount = session.prepare(
    s"select values[?] from $tableString where workspace = ? and namespace = ? and metric = ?" +
      s" and label = ?").setConsistencyLevel(ConsistencyLevel.ONE)

  private lazy val updateLabelValueCount = session.prepare(
    s"update $tableString set values += {?: ?} where workspace = ? and namespace = ? and metric = ?" +
      s" and label = ?").setConsistencyLevel(writeConsistencyLevel)


  override def createCql: String =
    s"""CREATE TABLE IF NOT EXISTS $tableString (
       |    workspace text,
       |    namespace text,
       |    metric text,
       |    label text,
       |    values map<text, int>,
       |    PRIMARY KEY ((workspace, namespace, metric), label)
       |) WITH compression = {'sstable_compression': '$sstableCompression'}""".stripMargin


  def incrementLabel(ws: String, ns: String, metric: String, labels: Map[String, String]): Future[Response] = {
    (labels - "_ws_" - "_ns_" - "_metric_").foreach {case (labelName, labelValue) => {
        val rs = session.execute(getMetricLabelsValueCount.bind(labelValue, ws, ns, metric, labelName))
        val count = if (rs.isExhausted) 0 else rs.one().getInt(0)
        updateLabelValueCount.bind(labelValue, count, ws, ns, metric, labelName)


       }
    }
    ???
  }
}
