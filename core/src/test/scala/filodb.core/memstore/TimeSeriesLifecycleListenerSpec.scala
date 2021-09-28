package filodb.core.memstore

import filodb.core.TestData
import filodb.core.binaryrecord2.{RecordBuilder, RecordSchema}
import filodb.core.metadata.{Dataset, DatasetOptions}
import filodb.memory.format.{UnsafeUtils, ZeroCopyUTF8String}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers


class TimeSeriesLifecycleListenerSpec  extends AnyFunSpec with Matchers {

  class TestListener extends TimeSeriesLifecycleListener {
      override def timeSeriesActivated(partKeyBase: Any, partKeyOffset: Long, partSchema: RecordSchema): Unit = {}
      override def timeSeriesDeactivated(partKeyBase: Any, partKeyOffset: Long, partSchema: RecordSchema): Unit = {}
  }

  it ("Given partition key, should extract labels and metric name") {
    val columns = Seq("timestamp:ts", "value:double")
    // Partition Key with multiple string columns
    val partitionColumns = Seq("_metric_:string", "_ws_:string", "_ns_:string", "app:string")
    val metricDataset = Dataset.make("tsdbdata",
      partitionColumns,
      columns,
      Seq.empty,
      None,
      DatasetOptions(Seq("_metric_", "_ns_", "_ws_"), "_metric_", true)).get
    val partKeyBuilder = new RecordBuilder(TestData.nativeMem, 2048)
    val defaultPartKey = partKeyBuilder.partKeyFromObjects(metricDataset.schema, "metric1", "ws", "ns", "app1")

    val testListener  = new TestListener()
    val labels = testListener.getLabelsFromPartitionKey(UnsafeUtils.ZeroPointer, defaultPartKey,
      metricDataset.partKeySchema)

    labels.keySet shouldEqual Set("_ns_", "_ws_", "_metric_", "app")
    labels("_ns_") shouldEqual "ns"
    labels("_metric_") shouldEqual "metric1"
    labels("_ws_") shouldEqual "ws"
    labels("app") shouldEqual "app1"
  }

  it ("Given partition key with metric name and map, should extract labels and metric name") {
    val columns = Seq("timestamp:ts", "value:double")

    // Partition Key with multiple string columns
    val partitionColumns = Seq("_metric_:string", "tags:map")
    val metricDataset = Dataset.make("tsdbdata",
      partitionColumns,
      columns,
      Seq.empty,
      None,
      DatasetOptions(Seq("_metric_", "_ns_", "_ws_"), "_metric_", true)).get
    val partKeyBuilder = new RecordBuilder(TestData.nativeMem, 2048)
    val tags = Map( ZeroCopyUTF8String("_ns_" )-> ZeroCopyUTF8String("ns"),
                    ZeroCopyUTF8String("_ws_" )-> ZeroCopyUTF8String("ws"),
                    ZeroCopyUTF8String("app" )-> ZeroCopyUTF8String("app1"))
    val defaultPartKey = partKeyBuilder.partKeyFromObjects(metricDataset.schema, "metric1", tags)

    val testListener  = new TestListener()
    val labels = testListener.getLabelsFromPartitionKey(UnsafeUtils.ZeroPointer, defaultPartKey,
      metricDataset.partKeySchema)

    labels.keySet shouldEqual Set("_ns_", "_ws_", "_metric_", "app")
    labels("_ns_") shouldEqual "ns"
    labels("_metric_") shouldEqual "metric1"
    labels("_ws_") shouldEqual "ws"
    labels("app") shouldEqual "app1"
  }

}
