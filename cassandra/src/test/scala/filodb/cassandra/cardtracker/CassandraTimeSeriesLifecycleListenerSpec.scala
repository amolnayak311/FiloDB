package filodb.cassandra.cardtracker

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
    listener.initialize()
    listener.partKeysTable.clearAll().futureValue
  }

  before {
    listener.partKeysTable.clearAll().futureValue
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
    // TODO: Assert other content
  }

}
