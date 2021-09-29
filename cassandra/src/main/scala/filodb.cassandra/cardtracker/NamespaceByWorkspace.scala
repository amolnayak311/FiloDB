package filodb.cassandra.cardtracker

import java.util.Collections

import scala.collection.JavaConverters.asScalaSet
import scala.concurrent.{ExecutionContext, Future}

import com.datastax.driver.core.ConsistencyLevel

import filodb.cassandra.FiloCassandraConnector
import filodb.cassandra.columnstore.BaseDatasetTable
import filodb.core.{DatasetRef, Response}



sealed class  NamespaceByWorkspace(val dataset: DatasetRef,
                                   val connector: FiloCassandraConnector,
                                   writeConsistencyLevel: ConsistencyLevel)
                                  (implicit ec: ExecutionContext)extends BaseDatasetTable {

  override def suffix: String = "ns_by_ws"

  override def createCql: String =
            s"""CREATE TABLE IF NOT EXISTS $tableString (
            | workspace text,
            | namespace set<text>,
            | PRIMARY KEY (workspace))
            | WITH compression = {'sstable_compression': '$sstableCompression'}""".stripMargin


  private lazy val writeNsToWs = session.prepare(
    s"update $tableString set namespace += ? where workspace = ?" ).setConsistencyLevel(writeConsistencyLevel)

  private lazy val getNsForWs = session.prepare(
    s"select namespace from $tableString where workspace = ?").setConsistencyLevel(ConsistencyLevel.ONE)

  def addNamespaceToWorkspace(workspace: String, namespace: String): Future[Response] =
    connector.execStmtWithRetries(writeNsToWs.bind(Collections.singleton(namespace), workspace))


  def getNamespacesForWorkspace(workspace: String): Set[String] = {
    val rsIter = session.execute(getNsForWs.bind(workspace)).iterator()
    if(rsIter.hasNext) asScalaSet(rsIter.next.getSet(0, classOf[String])).toSet else Set.empty
  }
}
