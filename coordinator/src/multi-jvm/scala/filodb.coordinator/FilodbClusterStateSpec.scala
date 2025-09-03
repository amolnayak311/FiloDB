package filodb.coordinator

import java.util.UUID
import scala.concurrent.duration._

import org.apache.pekko.remote.testkit.MultiNodeConfig
import com.typesafe.config.{Config, ConfigFactory}

class FilodbClusterStateSpecMultiJvmNode1 extends FilodbClusterStateSpec
class FilodbClusterStateSpecMultiJvmNode2 extends FilodbClusterStateSpec
class FilodbClusterStateSpecMultiJvmNode3 extends FilodbClusterStateSpec
class FilodbClusterStateSpecMultiJvmNode4 extends FilodbClusterStateSpec

abstract class FilodbClusterStateSpec extends ClusterSpec(FilodbClusterStateSpecMultiNodeConfig) {

  import FilodbClusterStateSpecMultiNodeConfig._

  override def initialParticipants = roles.size

  override def beforeAll(): Unit = {
    multiNodeSpecBeforeAll()
  }

  override def afterAll(): Unit = multiNodeSpecAfterAll()

  describe("FilodbCluster") {
    it("should initialize and join the cluster") {
      // Join cluster
      runOn(first) {
        cluster join node(first).address
        awaitCond(cluster.isJoined)
      }
      
      runOn(second, third, fourth) {
        cluster join node(first).address
        awaitCond(cluster.isJoined)
      }

      runOn(roles: _*) {
        within(30.seconds) {
          cluster.isInitialized should be(true)
          cluster.isJoined should be(true)
        }
      }
      enterBarrier("roles-up")
    }
    
    it("should have expected state when one node leaves") {
      runOn(second) {
        within(30.seconds) {
          info(s"leaving on $myself")
          cluster.cluster.leave(node(myself).address)
          awaitCond(!cluster.isJoined, 30.seconds)
          info(s"removed on $myself")
          awaitCond(cluster.cluster.isTerminated, 30.seconds)
          info(s"cluster terminated on $myself")
        }
      }

      runOn(roles.filterNot(_ == second): _*) {
        within(30.seconds) {
          awaitCond(cluster.cluster.state.members.size == 3, 30.seconds)
        }
      }
      enterBarrier("node-left")
    }
    
    it("should have expected state when one node is downed") {
      val victim = fourth
      
      runOn(victim) {
        within(30.seconds) {
          info(s"downing $myself")
          cluster.cluster.down(node(myself).address)
          awaitCond(!cluster.isJoined, 30.seconds)
          info(s"removed on $myself")
          awaitCond(cluster.cluster.isTerminated, 30.seconds)
          info(s"cluster terminated on $myself")
        }
      }

      runOn(first, third) {
        within(30.seconds) {
          awaitCond(cluster.cluster.state.members.size == 2, 30.seconds)
        }
      }
      enterBarrier("finished")
    }
  }
}

object FilodbClusterStateSpecMultiNodeConfig extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")
  val fourth = role("fourth")

  commonConfig(clusterConfig
    .withFallback(debugConfig(on = false)
    .withFallback(ConfigFactory.load("application_test.conf"))))

  def clusterConfig: Config = ConfigFactory.parseString(
    s"""
       |pekko.actor.provider = cluster
       |pekko.actor.warn-about-java-serializer-usage = off
       |pekko.cluster {
       |      jmx.enabled                         = off
       |      gossip-interval                     = 200 ms
       |      leader-actions-interval             = 200 ms
       |      unreachable-nodes-reaper-interval   = 500 ms
       |      periodic-tasks-initial-delay        = 300 ms
       |      publish-stats-interval              = 0 s # always, when it happens
       |      failure-detector.heartbeat-interval = 500 ms
       |}
       |
       |# Don't terminate ActorSystem via CoordinatedShutdown in tests
       |pekko.coordinated-shutdown.terminate-actor-system = off
       |pekko.coordinated-shutdown.run-by-jvm-shutdown-hook = off
       |pekko.cluster.run-coordinated-shutdown-when-down = off
       |
       |pekko.loglevel = INFO
       |pekko.log-dead-letters = off
       |pekko.log-dead-letters-during-shutdown = off
       |pekko.remote {
       |  log-remote-lifecycle-events = off
       |  artery.advanced.flight-recorder {
       |    enabled=on
       |    destination=target/flight-recorder-${UUID.randomUUID().toString}.afr
       |  }
       |}
       |pekko.test {
       |  single-expect-default = 5 s
       |}
    """.stripMargin)

  testTransport(on = true)
}