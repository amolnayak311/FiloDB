package filodb.cassandra.cardtracker

import scala.concurrent.{ExecutionContext, Future}

import com.datastax.driver.core.{ConsistencyLevel, Session}
import com.typesafe.config.Config
import com.typesafe.scalalogging.StrictLogging

import filodb.cassandra.{FiloCassandraConnector, FiloSessionProvider}
import filodb.core.{DatasetRef, Success, Response}
import filodb.core.binaryrecord2.RecordSchema
import filodb.core.memstore.TimeSeriesLifecycleListener


class CassandraTimeSeriesLifecycleListener(datasetRef: DatasetRef, config: Config)(implicit ec: ExecutionContext)
  extends TimeSeriesLifecycleListener with StrictLogging {

  val cassandraConfig: Config = config.getConfig("cassandra")
  val session: Session = FiloSessionProvider.openSession(cassandraConfig)

  val clusterConnector = new FiloCassandraConnector {
      def config: Config = cassandraConfig
      def session: Session = CassandraTimeSeriesLifecycleListener.this.session
      def ec: ExecutionContext = CassandraTimeSeriesLifecycleListener.this.ec

      val keyspace: String = config.getString("keyspace")
  }

  val partKeysTable = new PartKeys(datasetRef, clusterConnector, ConsistencyLevel.LOCAL_QUORUM)
  val nsByWsTable = new NamespaceByWorkspace(datasetRef, clusterConnector, ConsistencyLevel.LOCAL_QUORUM)
  val metricsByWsNs = new MetricsByWorkspaceAndNamespace(datasetRef, clusterConnector, ConsistencyLevel.LOCAL_QUORUM)


  override def timeSeriesActivated(partKeyBase: Any, partKeyOffset: Long, partSchema: RecordSchema): Unit = {
    // All calls to these callback are sequential and not concurrent as these callbacks are invoked from TimeSeriesShard

    // 1. Check if the given part key is present in the database, the reason this check is necessary because,
    // this callback will be invoked each time TSPartition instance is created. Since this datastore is external to
    // the shard, it is possible we receive duplicate callbacks when a shard restarts, this deduplication is thus
    // necessary
    val partKey = partSchema.asByteArray(partKeyBase, partKeyOffset)

    if(!partKeysTable.partitionExists(partKey)) {
        // 2. Partition is not recorded in cardinality tracker, record it now
        partKeysTable.writePartKey(partKey)
        val labelMap = getLabelsFromPartitionKey(partKeyBase, partKeyOffset, partSchema)
        // TODO: use Global constants, where are they defined?
        nsByWsTable.addNamespaceToWorkspace(labelMap("_ws_"), labelMap("_ns_"))
        metricsByWsNs.incrementMetricCount(labelMap("_ws_"), labelMap("_ns_"), labelMap("_metric_"))

    } else {
      // Simply update the lastUpdated timestamp of the partition
      // In Cassandra insertion of row with with same primary key replaces the row, in this case
      // the lastupdated" column gets updated
      partKeysTable.writePartKey(partKey)
    }
  }

  override def timeSeriesDeactivated(partKeyBase: Any, partKeyOffset: Long, partSchema: RecordSchema): Unit = ???


  def initialize(): Future[Response] =
    for {pkResp <- partKeysTable.initialize() if pkResp == Success
          wsTable <- nsByWsTable.initialize()   if wsTable == Success
          metricsTable <- metricsByWsNs.initialize() if metricsTable == Success}
      yield Success

  def shutdown(): Unit = {
    session.close()
  }

}
