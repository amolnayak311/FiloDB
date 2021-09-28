package filodb.cassandra.cardtracker

import java.lang.{Long => JLong}

import com.datastax.driver.core.ConsistencyLevel
import scala.concurrent.{ExecutionContext, Future}

import filodb.cassandra.FiloCassandraConnector
import filodb.cassandra.columnstore.BaseDatasetTable
import filodb.core.{DatasetRef, Response}

sealed class PartKeys(val dataset: DatasetRef,
                      val connector: FiloCassandraConnector,
                      writeConsistencyLevel: ConsistencyLevel)
                     (implicit ec: ExecutionContext)extends BaseDatasetTable{

  import filodb.cassandra.Util._


  override def suffix: String = "part_keys"

  val createCql: String =
    s"""CREATE TABLE IF NOT EXISTS $tableString (
       |    partKey blob,
       |    lastupdated bigint,
       |    PRIMARY KEY (partKey)
       |) WITH compression = {'chunk_length_in_kb': '16', 'sstable_compression': '$sstableCompression'}""".stripMargin


  private lazy val writePartitionCql = session.prepare(
    s"INSERT INTO $tableString (partKey, lastupdated) " +
      s"VALUES (?, ?)")
    .setConsistencyLevel(writeConsistencyLevel)

  private lazy val countCql = session.prepare(
    s"SELECT count(1) FROM $tableString " +
      s"WHERE partKey = ? ")
    .setConsistencyLevel(ConsistencyLevel.ONE)

  def writePartKey(partKey: Array[Byte]): Future[Response] =
      connector.execStmtWithRetries(writePartitionCql.bind(toBuffer(partKey), System.currentTimeMillis(): JLong))


  def partitionExists(partKey: Array[Byte]): Boolean =
    session.execute(countCql.bind(toBuffer(partKey))).one().getLong("count") != 0


}
