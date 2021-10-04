package filodb.cassandra.cardtracker

import scala.collection.JavaConverters.asScalaSet
import filodb.cassandra.AllTablesTest
import filodb.core.TestData
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.{Dataset, Schemas}
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String}
import org.scalatest.matchers.should.Matchers

import java.nio.ByteBuffer

class CassandraTimeSeriesLifecycleListenerSpec extends AllTablesTest with Matchers {


  val promDataset = Dataset("card_tracker", Schemas.promCounter)
  val listener = new CassandraTimeSeriesLifecycleListener(promDataset.ref, config)(scheduler)

  // First create the tables in C*
  override def beforeAll(): Unit = {
    super.beforeAll()
    listener.initialize().futureValue
  }

  before {
    listener.partKeysTable.clearAll().futureValue
    listener.nsByWsTable.clearAll().futureValue
    listener.metricsByWsNs.clearAll().futureValue
  }

  it ("should given new time series part key, save the new part key in cassandra") {
    val partKeyBuilder = new RecordBuilder(TestData.nativeMem, 2048)
    val tags = Map(ZeroCopyUTF8String("_ns_") -> ZeroCopyUTF8String("ns"),
      ZeroCopyUTF8String("_ws_") -> ZeroCopyUTF8String("ws"),
      ZeroCopyUTF8String("app") -> ZeroCopyUTF8String("app1"))
    val defaultPartKey = partKeyBuilder.partKeyFromObjects(Schemas.promCounter, "metric1", tags)
    val partKey = Schemas.promCounter.partKeySchema.asByteArray(UnsafeUtils.ZeroPointer, defaultPartKey)
    // Get count initially
    val stmt = session.prepare("select count(1) from unittest.card_tracker_part_keys where partkey = ?")
      .bind(ByteBuffer.wrap(partKey))
    assert(session.execute(stmt).one().getLong(0) == 0)
    listener.timeSeriesActivated(UnsafeUtils.ZeroPointer, defaultPartKey, Schemas.promCounter.partKeySchema)
    assert(session.execute(stmt).one().getLong(0) == 1)

    val res = session.execute("select namespace from unittest.card_tracker_ns_by_ws where workspace = 'ws'")
    asScalaSet(res.one().getSet(0, classOf[String])) shouldEqual Set("ns")
    listener.nsByWsTable.getNamespacesForWorkspace("ws") shouldEqual Set("ns")

    val numts = session.execute("select numts from unittest.card_tracker_metrics_by_ws_ns " +
      "where workspace = 'ws' and namespace = 'ns' and metric = 'metric1'")

    numts.one().getInt(0) shouldEqual 1

    // TODO: Assert other content
  }

  it ("should ensure timeSeriesActivated is idempotent") {
    val partKeyBuilder = new RecordBuilder(TestData.nativeMem, 2048)
    val tags = Map(ZeroCopyUTF8String("_ns_") -> ZeroCopyUTF8String("ns"),
      ZeroCopyUTF8String("_ws_") -> ZeroCopyUTF8String("ws"),
      ZeroCopyUTF8String("app") -> ZeroCopyUTF8String("app1"))
    val defaultPartKey = partKeyBuilder.partKeyFromObjects(Schemas.promCounter, "metric1", tags)
    val partKey = Schemas.promCounter.partKeySchema.asByteArray(UnsafeUtils.ZeroPointer, defaultPartKey)
    // Get count initially
    val stmt = session.prepare("select count(1) from unittest.card_tracker_part_keys where partkey = ?")
      .bind(ByteBuffer.wrap(partKey))
    assert(session.execute(stmt).one().getLong(0) == 0)
    listener.timeSeriesActivated(UnsafeUtils.ZeroPointer, defaultPartKey, Schemas.promCounter.partKeySchema)
    assert( session.execute(stmt).one().getLong(0) == 1)

    listener.timeSeriesActivated(UnsafeUtils.ZeroPointer, defaultPartKey, Schemas.promCounter.partKeySchema)
    assert(session.execute(stmt).one().getLong(0) == 1)

    // Assert the namespace is still listed under the given ws
    val res = session.execute("select namespace from unittest.card_tracker_ns_by_ws where workspace = 'ws'")
    asScalaSet(res.one().getSet(0, classOf[String])) shouldEqual Set("ns")
    listener.nsByWsTable.getNamespacesForWorkspace("ws") shouldEqual Set("ns")

    // Assert the number of unique metric names is still 1 as the activate ts call is idempotent

    val numts = session.execute("select numts from unittest.card_tracker_metrics_by_ws_ns " +
          " where workspace = 'ws' and namespace = 'ns' and metric = 'metric1'")
    numts.one().getInt(0) shouldEqual 1


    // TODO: Assert other content
  }

}
