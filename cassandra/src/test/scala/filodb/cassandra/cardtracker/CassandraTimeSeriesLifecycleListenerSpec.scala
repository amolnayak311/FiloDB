package filodb.cassandra.cardtracker

import java.nio.ByteBuffer

import scala.collection.JavaConverters.asScalaSet

import org.scalatest.matchers.should.Matchers

import filodb.cassandra.AllTablesTest
import filodb.core.TestData
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.{Dataset, Schemas}
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String}

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
    listener.metricLabelCardinality.clearAll().futureValue
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

    val labelValCardinality = session.execute("select values from unittest.card_tracker_metric_label_cardinality " +
      "where workspace = 'ws' and namespace = 'ns' and metric = 'metric1' and label = 'app'")

    // Check the expected cardinality
    labelValCardinality.isExhausted shouldEqual false
    val dict = labelValCardinality.one().getMap(0, classOf[String], classOf[Integer])
    dict.size() shouldEqual 1
    dict.containsKey("app1") shouldEqual true
    dict.get("app1") shouldEqual 1
  }

  it ("should given two time series part key for same metric name, save cardinality correctly in cassandra") {
    val partKeyBuilder1 = new RecordBuilder(TestData.nativeMem, 2048)
    val tags1 = Map(ZeroCopyUTF8String("_ns_") -> ZeroCopyUTF8String("ns"),
      ZeroCopyUTF8String("_ws_") -> ZeroCopyUTF8String("ws"),
      ZeroCopyUTF8String("app") -> ZeroCopyUTF8String("app1"))
    val partKey1 = partKeyBuilder1.partKeyFromObjects(Schemas.promCounter, "metric1", tags1)
    val partKeyBytes1 = Schemas.promCounter.partKeySchema.asByteArray(UnsafeUtils.ZeroPointer, partKey1)

    val partKeyBuilder2 = new RecordBuilder(TestData.nativeMem, 2048)
    val tags2 = Map(ZeroCopyUTF8String("_ns_") -> ZeroCopyUTF8String("ns"),
      ZeroCopyUTF8String("_ws_") -> ZeroCopyUTF8String("ws"),
      ZeroCopyUTF8String("app") -> ZeroCopyUTF8String("app1"),
      ZeroCopyUTF8String("pod") -> ZeroCopyUTF8String("my-pod"))
    val partKey2 = partKeyBuilder2.partKeyFromObjects(Schemas.promCounter, "metric1", tags2)
    val partKeyBytes2 = Schemas.promCounter.partKeySchema.asByteArray(UnsafeUtils.ZeroPointer, partKey2)


    // Get count initially
    assert(session.execute("select count(1) from unittest.card_tracker_part_keys").one().getLong(0) == 0)

    listener.timeSeriesActivated(UnsafeUtils.ZeroPointer, partKey1, Schemas.promCounter.partKeySchema)
    listener.timeSeriesActivated(UnsafeUtils.ZeroPointer, partKey2, Schemas.promCounter.partKeySchema)

    assert(session.execute("select count(1) from unittest.card_tracker_part_keys").one().getLong(0) == 2)

    val res = session.execute("select namespace from unittest.card_tracker_ns_by_ws where workspace = 'ws'")
    asScalaSet(res.one().getSet(0, classOf[String])) shouldEqual Set("ns")
    listener.nsByWsTable.getNamespacesForWorkspace("ws") shouldEqual Set("ns")

    val numts = session.execute("select numts from unittest.card_tracker_metrics_by_ws_ns " +
      "where workspace = 'ws' and namespace = 'ns' and metric = 'metric1'")

    numts.one().getInt(0) shouldEqual 2

    val appLabelCardinality = session.execute("select values from unittest.card_tracker_metric_label_cardinality " +
      "where workspace = 'ws' and namespace = 'ns' and metric = 'metric1' and label = 'app'")

    // Check the expected cardinality
    appLabelCardinality.isExhausted shouldEqual false
    val dict = appLabelCardinality.one().getMap(0, classOf[String], classOf[Integer])
    dict.size() shouldEqual 1
    dict.containsKey("app1") shouldEqual true
    dict.get("app1") shouldEqual 2

    val podLabelCardinality = session.execute("select values from unittest.card_tracker_metric_label_cardinality " +
      "where workspace = 'ws' and namespace = 'ns' and metric = 'metric1' and label = 'pod'")

    val dict1 = podLabelCardinality.one().getMap(0, classOf[String], classOf[Integer])
    dict1.size() shouldEqual 1
    dict1.containsKey("my-pod") shouldEqual true
    dict1.get("my-pod") shouldEqual 1
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

    val labelValCardinality = session.execute("select values from unittest.card_tracker_metric_label_cardinality " +
      "where workspace = 'ws' and namespace = 'ns' and metric = 'metric1' and label = 'app'")

    // Check the expected cardinality
    labelValCardinality.isExhausted shouldEqual false
    val dict = labelValCardinality.one().getMap(0, classOf[String], classOf[Integer])
    dict.size() shouldEqual 1
    dict.containsKey("app1") shouldEqual true
    dict.get("app1") shouldEqual 1
  }

}
