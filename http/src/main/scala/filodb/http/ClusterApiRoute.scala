package filodb.http

import com.typesafe.scalalogging.StrictLogging
import io.circe.{Decoder, Encoder, Printer}
import io.circe.parser.decode
import org.apache.pekko.actor.ActorRef
import org.apache.pekko.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller, ToResponseMarshallable}
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes => Codes}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}

import filodb.coordinator.{CurrentShardSnapshot, NodeClusterActor}
import filodb.core.{DatasetRef, ErrorResponse, Success => SuccessResponse}
import filodb.core.store.{AssignShardConfig, UnassignShardConfig}
import filodb.http.apiv1.{HttpSchema, HttpShardState}

class ClusterApiRoute(clusterProxy: ActorRef) extends FiloRoute with StrictLogging {
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
  import filodb.coordinator.client.Client._
  import NodeClusterActor._

  val route = pathPrefix("api" / "v1" / "cluster") {
    path(Segment / "status") { dataset =>
      get {
        onSuccess(asyncAsk(clusterProxy, GetShardMap(DatasetRef.fromDotString(dataset)))) {
          case CurrentShardSnapshot(_, map) =>
            val statusList = map.shardValues.zipWithIndex.map { case ((ref, status), idx) =>
              HttpShardState(idx, status.toString,
                if (ref == ActorRef.noSender) "" else ref.path.address.toString)
            }
            complete(ToResponseMarshallable(httpList(statusList)))
          case DatasetUnknown(_) =>
            complete(ToResponseMarshallable(Codes.NotFound ->
              httpErr("DatasetUnknown", s"Dataset $dataset is not registered")))
          case InternalServiceError(errorMessage) =>
            complete(ToResponseMarshallable(Codes.InternalServerError -> httpErr("InternalServerError", errorMessage)))
        }
      }
    } ~
      path(Segment / "stopshards") { dataset =>
        post {
          entity(as[UnassignShardConfig]) { shardConfig =>
            try onSuccess(asyncAsk(clusterProxy, StopShards(shardConfig, DatasetRef.fromDotString(dataset)))) {
              case SuccessResponse =>
                complete(ToResponseMarshallable(httpList(Seq.empty[String])))
              case e: ErrorResponse =>
                complete(ToResponseMarshallable(Codes.BadRequest -> httpErr(e.toString, e.toString)))
            } catch {
              case e: Exception =>
                complete(ToResponseMarshallable(Codes.InternalServerError -> httpErr(e)))
            }
          }
        }
      } ~
      path(Segment / "startshards") { dataset =>
        post {
          entity(as[AssignShardConfig]) { shardConfig =>
            try onSuccess(asyncAsk(clusterProxy, StartShards(shardConfig, DatasetRef.fromDotString(dataset)))) {
              case SuccessResponse =>
                complete(ToResponseMarshallable(httpList(Seq.empty[String])))
              case e: ErrorResponse =>
                complete(ToResponseMarshallable(Codes.BadRequest -> httpErr(e.toString, e.toString)))
            } catch {
              case e: Exception =>
                complete(ToResponseMarshallable(Codes.InternalServerError -> httpErr(e)))
            }
          }
        }
      }
  }
}
