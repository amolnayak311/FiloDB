package filodb.cassandra.cardtracker

import java.lang.{Integer => JInteger}

import scala.concurrent.{ExecutionContext, Future}

import com.datastax.driver.core.ConsistencyLevel

import filodb.cassandra.FiloCassandraConnector
import filodb.cassandra.columnstore.BaseDatasetTable
import filodb.core.{DatasetRef, Response}

sealed class MetricsByWorkspaceAndNamespace(val dataset: DatasetRef,
                                            val connector: FiloCassandraConnector,
                                            writeConsistencyLevel: ConsistencyLevel)
                                           (implicit ec: ExecutionContext)extends BaseDatasetTable {

  override def suffix: String = "metrics_by_ws_ns"

  override def createCql: String =
    s"""CREATE TABLE IF NOT EXISTS $tableString (
       |    workspace text,
       |    namespace text,
       |    metric text,
       |    numts int,
       |    PRIMARY KEY ((workspace, namespace), metric)
       |) WITH compression = {'sstable_compression': '$sstableCompression'}""".stripMargin


  private lazy val updateCountOfMetric = session.prepare(
    s"update $tableString set numts = ? where workspace = ? and namespace = ?" +
      s" and metric = ?").setConsistencyLevel(writeConsistencyLevel)

  private lazy val getMetricCount = session.prepare(
    s"select numts from $tableString where workspace = ? and namespace = ?" +
      s" and metric = ?").setConsistencyLevel(ConsistencyLevel.ONE)

  def incrementMetricCount(workspace: String, namespace: String, metricName: String): Future[Response] = {

    // TODO: Can we assume this call is single threaded for a given workspace, namespace and metricName
    //  or do we need to use LWT (Light Weight Transactions)? LWT uses consensus algorithms and are slow
    //  Using counters makes increments atomic but non idempotent and deleting counters will have undesired behavior
    val iter = session.execute(getMetricCount.bind(workspace, namespace, metricName)).iterator();
    val currentCount = if (iter.hasNext) iter.next().getLong(0).toInt else 0
    // Using with retries doesn't make too much sense as the previous query is executed without retries
    connector.execStmt(updateCountOfMetric.bind(currentCount + 1: JInteger, workspace, namespace, metricName))
  }

}
