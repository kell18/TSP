package ru.itclover.tsp.http.utils

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import com.typesafe.scalalogging.Logger
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.{Matchers, Suite}
import ru.itclover.tsp.http.HttpService
import ru.itclover.tsp.http.domain.output.SuccessfulResponse.FinishedJobResponse
import scala.concurrent.ExecutionContextExecutor
import scala.util.Success
import scala.concurrent.duration.DurationInt

trait HttpServiceMathers extends ScalatestRouteTest with Matchers with HttpService { self: Suite =>

  implicit override val executionContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.global
  implicit override val streamEnvironment = StreamExecutionEnvironment.createLocalEnvironment()
  streamEnvironment.setMaxParallelism(30000) // For proper keyBy partitioning

  implicit def defaultTimeout(implicit system: ActorSystem) = RouteTestTimeout(300.seconds)

  val log = Logger("HttpServiceMathers")

  /** Util for checking segments count and size in seconds */
  def checkByQuery(expectedValues: Seq[Double], query: String, epsilon: Double = 0.0001)
                  (implicit container: JDBCContainer): Unit = {
    val resultSet = container.executeQuery(query)
    for (expectedVal <- expectedValues) {
      resultSet.next() shouldEqual true
      val value = resultSet.getDouble(1)
      value should === (expectedVal +- epsilon)
    }
  }

  def checkAndGetExecTimeSec(): Long = {
    status shouldEqual StatusCodes.OK
    val resp = unmarshal[FinishedJobResponse](responseEntity)
    resp shouldBe a[Success[_]]
    val execTimeS = resp.get.response.execTimeSec
    log.info(s"Test job completed for $execTimeS sec.")
    execTimeS
  }
}
