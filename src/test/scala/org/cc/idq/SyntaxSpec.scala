package org.cc.idq

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, Executors, TimeUnit}

import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.duration._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}

class SyntaxSpec extends AnyFlatSpec {

  import Syntax._

  "Syntax.acquire" should "allow to write the acquisition of an id without having to explicitly release it" in {
    val executor = Executors.newFixedThreadPool(8)
    implicit val ec = ExecutionContext.fromExecutor(executor)

    val list = new ConcurrentLinkedQueue[Int]()
    val idq = new IdQueue[Int]()
    val reserve = acquire[Int, Unit](idq) _

    (1 to 10).foreach { i =>
      reserve(1) { id =>
        for {
          _ <- doSomeCallToService("whatever value", delay = 100.millis)
        } yield list.add(i)
      }
    }
    val latch = new CountDownLatch(1)
    reserve(1) { _ => Future { latch.countDown() } }

    latch.await(5, TimeUnit.SECONDS)
    assert(latch.getCount==0)
    assert(list.toArray.toList === (1 to 10).toList)

    // Cleanup
    executor.shutdownNow()
  }



  private[this] def doSomeCallToService[T](value: T, delay: Duration = 1.second)(implicit ec: ExecutionContext): Future[T] =
    Future {
      Thread.sleep(delay.toMillis); value
    }

}
