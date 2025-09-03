package filodb.coordinator

import org.apache.pekko.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import org.apache.pekko.testkit.ImplicitSender
import com.typesafe.scalalogging.StrictLogging
import filodb.core.AsyncTest
import org.scalatest.funspec.AnyFunSpecLike

abstract class ClusterSpec(config: MultiNodeConfig) extends MultiNodeSpec(config)
  with AnyFunSpecLike with StrictLogging with ImplicitSender with AsyncTest {

  val cluster = FilodbCluster(system)
}
