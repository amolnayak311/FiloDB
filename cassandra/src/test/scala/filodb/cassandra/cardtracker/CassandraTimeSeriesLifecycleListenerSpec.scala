package filodb.cassandra.cardtracker

import filodb.cassandra.AllTablesTest
import filodb.core.TestData
import filodb.core.binaryrecord2.RecordBuilder
import filodb.core.metadata.{Dataset, Schemas}
import filodb.memory.format.{ZeroCopyUTF8String, UnsafeUtils}

class CassandraTimeSeriesLifecycleListenerSpec extends AllTablesTest {


  val promDataset = Dataset("card_tracker", Schemas.promCounter)
  val listener = new CassandraTimeSeriesLifecycleListener(promDataset.ref, config)(scheduler)

  // First create the tables in C*
  override def beforeAll(): Unit = {
    super.beforeAll()
    listener.initialize()
    listener.partKeysTable.clearAll().futureValue
  }

  it ("should given new time series part key, should save the contents in cassandra") {
    val partKeyBuilder = new RecordBuilder(TestData.nativeMem, 2048)
    val tags = Map(ZeroCopyUTF8String("_ns_") -> ZeroCopyUTF8String("ns"),
      ZeroCopyUTF8String("_ws_") -> ZeroCopyUTF8String("ws"),
      ZeroCopyUTF8String("app") -> ZeroCopyUTF8String("app1"))
    val defaultPartKey = partKeyBuilder.partKeyFromObjects(Schemas.promCounter, "metric1", tags)
    listener.timeSeriesActivated(UnsafeUtils.ZeroPointer, defaultPartKey, Schemas.promCounter.partKeySchema)
    println(listener.partKeysTable.partitionExists(
      Schemas.promCounter.partKeySchema.asByteArray(UnsafeUtils.ZeroPointer, defaultPartKey)))
  }

}
