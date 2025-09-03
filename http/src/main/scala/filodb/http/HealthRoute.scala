package filodb.http

import com.typesafe.scalalogging.StrictLogging
import io.circe.{Decoder, Encoder, Printer}
import io.circe.parser.decode
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}

import filodb.coordinator._
import filodb.coordinator.client.Client._
import filodb.coordinator.v2.{DatasetShardHealth, LocalShardsHealthRequest}
import filodb.core.DatasetRef
import filodb.http.apiv1.HttpSchema

final case class DatasetEvents(dataset: String, shardEvents: Seq[String])

class HealthRoute(coordinatorActor: ActorRef, v2ClusterEnabled: Boolean, settings: HttpSettings)
  extends FiloRoute with StrictLogging {

  // Circe support for Pekko HTTP
  implicit def circeJsonMarshaller[A](implicit encoder: Encoder[A],
                                      printer: Printer = Printer.noSpaces): ToEntityMarshaller[A] =
    Marshaller.withFixedContentType(ContentTypes.`application/json`) { obj =>
      HttpEntity(ContentTypes.`application/json`, printer.pretty(encoder(obj)))
    }

  implicit def circeJsonUnmarshaller[A](implicit decoder: Decoder[A]): FromEntityUnmarshaller[A] =
    Unmarshaller.byteStringUnmarshaller
      .forContentTypes(ContentTypes.`application/json`)
      .mapWithCharset { (data, charset) =>
        val input = if (charset.nioCharset == java.nio.charset.StandardCharsets.UTF_8) data.utf8String
                   else data.decodeString(charset.nioCharset.name)
        decode[A](input).fold(throw _, identity)
      }

  import io.circe.generic.auto._

  import HttpSchema._

  private val healthyShardStatuses = Seq(ShardStatusActive.getClass,
                                         ShardStatusRecovery.getClass,
                                         ShardStatusAssigned.getClass)

  /**
   * Checks if the shardEvents are in active or recovery phase. If so, returns true else false.
   * We also make sure that all the responses from the shards assigned to the local node is received before we
   * continue with the liveness check
   * @param shardEvents
   * @return true if all local shards are live else false
   */
  def checkIfAllShardsLiveClusterV1(shardEvents: collection.Map[DatasetRef, Seq[ShardEvent]]): Boolean = {
    val allShardEvents = shardEvents.values.flatten.toSeq
    // wait for the minimum amount of responses needed
    if (allShardEvents.length < numResponsesNeededForLivenessCheck) {
      return false
    }
    allShardEvents.forall {
      case IngestionStarted(_, _, _) | RecoveryStarted(_, _, _, _) | RecoveryInProgress(_, _, _, _) => true
      case _ => false
    }
  }

  /**
   * Checks if the shard status are in active or recovery phase. If so, returns true else false
   * We also make sure that all the responses from the shards assigned to the local node is received before we
   * continue with the liveness check
   * @param shardHealthSeq
   * @return true if all local shards are live else false
   */
  def checkIfAllShardsLiveClusterV2(shardHealthSeq: Seq[DatasetShardHealth]): Boolean = {
    val allShardStatus = shardHealthSeq.map(x => x.status)
    // wait for the minimum amount of responses needed
    if (allShardStatus.length < numResponsesNeededForLivenessCheck) {
      return false
    }
    allShardStatus.forall {
      case ShardStatusActive | ShardStatusRecovery(_) => true
      case _ => false
    }
  }

  /**
   * Gets the total number of shards from all the dataset configs and calculates the
   * number of responses needed for liveness check
   * @return [Int] number of responses needed
   */
  lazy val numResponsesNeededForLivenessCheck : Int = {
    var totalNumShards = 0
    settings.filoSettings.streamConfigs.foreach { config =>
      totalNumShards += config.getInt("num-shards")
    }
    totalNumShards / settings.filoSettings.minNumNodes.get
  }

  val route = {
    // Returns the local dataset/shard status as JSON
    path ("__health") {
      get {
        if (v2ClusterEnabled) {
          onSuccess(asyncAsk(coordinatorActor, LocalShardsHealthRequest, settings.queryAskTimeout)) {
            case m: Seq[DatasetShardHealth]@unchecked =>
              if (m.forall(h => healthyShardStatuses.contains(h.status.getClass))) complete(m)
              else {
                logger.error(s"Health check for this FiloDB node failed since some shards were not healthy: " +
                  s"$m")
                complete(StatusCodes.ServiceUnavailable -> m)
              }
          }
        } else {
          onSuccess(asyncAsk(coordinatorActor, StatusActor.GetCurrentEvents, settings.queryAskTimeout)) {
            case m: collection.Map[DatasetRef, Seq[ShardEvent]]@unchecked =>
              complete(httpList(m.toSeq.map { case (ref, ev) => DatasetEvents(ref.toString, ev.map(_.toString)) }))
          }
        }
      }
    } ~
    // Adding a simple liveness endpoint to check if the FiloDB instance is reachable and if the kafka and cassandra
    // connections are initialized correctly on startup. This will help us in detecting any connection issues to
    // kafka and cassandra on startup.
    // NOTE: We don't wait for bootstrap to finish or normal ingestion to start or shards to be active. As soon as
    // ingestion or recovery is started, we consider the filodb pods to be live
    path ("__liveness") {
      get {
        if (v2ClusterEnabled) {
          onSuccess(asyncAsk(coordinatorActor, LocalShardsHealthRequest, settings.queryAskTimeout)) {
            case m: Seq[DatasetShardHealth]@unchecked =>
              if (checkIfAllShardsLiveClusterV2(m)) {
                complete(httpLiveness("UP"))
              }
              else {
                logger.error(s"Liveness check for node failed ! ShardStatus: $m")
                complete(httpLiveness("DOWN"))
              }
          }
        } else {
          onSuccess(asyncAsk(coordinatorActor, StatusActor.GetCurrentEvents, settings.queryAskTimeout)) {
            case m: collection.Map[DatasetRef, Seq[ShardEvent]]@unchecked =>
              if (checkIfAllShardsLiveClusterV1(m)) {
                complete(httpLiveness("UP"))
              }
              else {
                logger.error(s"Liveness check for node failed ! ShardStatus: $m")
                complete(httpLiveness("DOWN"))
              }
          }
        }
      }
    }
  }
}
