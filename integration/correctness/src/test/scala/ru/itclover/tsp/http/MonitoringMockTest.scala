package ru.itclover.tsp.http
import akka.actor.ActorSystem
import akka.http.scaladsl.{Http, model}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.{HttpRequest, StatusCodes, Uri}
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.ActorMaterializer
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{AsyncFlatSpec, BeforeAndAfter, BeforeAndAfterAll, Matchers}
import ru.itclover.tsp.http.routes.MonitoringRoutes
import ru.itclover.tsp.http.services.flink.MonitoringService
import ru.itclover.tsp.http.services.flink.MonitoringServiceModel.{JobBrief, JobsOverview, MetricInfo}
import ru.itclover.tsp.http.utils.MockServer
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class MonitoringMockTest
    extends AsyncFlatSpec
    with ScalatestRouteTest
    with Matchers
    with Directives
    with BeforeAndAfter
    with ScalaFutures {

  val port = 9034
  private var t: Thread = _

  before {
    t = new Thread(() => MockServer.startServer("127.0.0.1", port))
    t.start()
    Thread.sleep(1000)
  }

  after {
    t.join(1000)
  }

  "Monitoring service" should "work with mocked Flink service" in {
    val monitoringService = MonitoringService(s"http://127.0.0.1:$port")
    monitoringService.queryJobsOverview.map { res => assert(res.jobs.length == 2) }

    monitoringService.queryJobByName("job1").map { res => assert(res.isDefined) }
    monitoringService.queryJobByName("job2").map { res => assert(res.isDefined) }
    monitoringService.queryJobByName("job3").map { res => assert(res.isEmpty) }

    monitoringService.queryJobExceptions("one").map { res => assert(res.isEmpty) }
    monitoringService.queryJobInfo("job1").map { res => assert(res.map(x => x.jid).getOrElse("error") == "1") }
    monitoringService.queryJobAllMetrics("job1").map { res => assert(res.map(_ == Map.empty).getOrElse(false)) }
    monitoringService.queryJobAllMetrics("job3").map { res => assert(res.isLeft) }
    monitoringService.queryJobDetailsWithMetrics("job1", List(MetricInfo(0, "metric1.1", "metric1.1"))).map { res => assert(res.isDefined) }

    monitoringService.sendStopQuery("job1").map { res => assert(res.isDefined) }
    monitoringService.sendStopQuery("job2").map { res => assert(res.isDefined) }
    monitoringService.sendStopQuery("job3").map { res => assert(res.isEmpty) }
  }

  "Monitoring routes" should "work" in {
    val monitoringRoutes = new MonitoringRoutes {
      override implicit val actors: ActorSystem = ActorSystem("TSP-monitoring-test")
      override implicit val materializer: ActorMaterializer = ActorMaterializer()(actors)
      override implicit val executionContext: ExecutionContextExecutor = system.dispatcher
      override val uri: Uri = s"http://127.0.0.1:$port"
}
    Get("/metainfo/getVersion") ~> monitoringRoutes.route ~> check {
      response.status shouldBe StatusCodes.OK
    }

    Get("/jobs/overview") ~> monitoringRoutes.route ~> check {
      response.status shouldBe StatusCodes.OK
    }

    Get("/job/3/statusAndMetrics") ~> monitoringRoutes.route ~> check {
      response.status shouldBe StatusCodes.BadRequest
    }

    Get("/job/3/status") ~> monitoringRoutes.route ~> check {
      response.status shouldBe StatusCodes.BadRequest
    }

    Get("/job/3/exceptions") ~> monitoringRoutes.route ~> check {
      response.status shouldBe StatusCodes.BadRequest
    }

    Get("/job/3/stop") ~> monitoringRoutes.route ~> check {
      response.status shouldBe StatusCodes.BadRequest
    }
  }
}
