package filodb.http

import ch.qos.logback.classic.{Level, Logger}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.slf4j.LoggerFactory

object AdminRoutes extends FiloRoute {
  val route = pathPrefix("admin") {
    // POST /admin/loglevel/<loggerName> - data {level}
    path("loglevel" / Segment) { loggerName =>
      post {
        entity(as[String]) { newLevel =>
          val logger = LoggerFactory.getLogger(loggerName).asInstanceOf[Logger]
          val level = Level.valueOf(newLevel)
          logger.setLevel(level)
          complete(s"Changed log level for $logger to $level")
        }
      }
    }
  }

}
