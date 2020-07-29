package org.cc.idq

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, Executors, TimeUnit}

import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

import scala.jdk.CollectionConverters._

class IdQueueTest extends AnyFlatSpec {

  "IdQueue.acquire" should "access directly the id (without waiting) when id is not being processed" in {
    val executor = Executors.newFixedThreadPool(2)
    implicit val ec = ExecutionContext.fromExecutor(executor)

    val latch = new CountDownLatch(1)
    val idq = new IdQueue[Int]()
    // Usage
    (for {
      _ <- idq.acquire(id = 1)
      _ <- doSomeCallToService(value = 1)
      _ <- doSomeCallToService(value = 2)
    } yield latch.countDown())
      .onComplete {
        case _ => idq.release(id = 1)
      }
    // If id is not available, this will timeout because nobody will release the id
    latch.await(5, TimeUnit.SECONDS)
    assert(latch.getCount == 0)

    // Cleanup
    executor.shutdownNow()
  }

  it should "not make wait for an id when a different id is being processed" in {
    val executor = Executors.newFixedThreadPool(2)
    implicit val ec = ExecutionContext.fromExecutor(executor)

    val latch = new CountDownLatch(1)
    val idq = new IdQueue[Int]()
    // Calculation 1 starts first but calculation 2 will terminate first
    val calc1 = simulateCalculation(idq, id = 1, latch)
    val calc2 = simulateCalculation(idq, id = 2)

    Await.result(calc2, 3.seconds)
    assert(calc2.isCompleted)
    assert(idq.isBusy(id = 1))
    assert(!calc1.isCompleted)
    // Allowing Calculation 1 to terminate
    latch.countDown()
    Await.result(calc1, 3.seconds)
    assert(calc1.isCompleted)

    // Cleanup
    executor.shutdownNow()
  }

  "IdQueue.acquire" should "force a calculation to wait if its id is already being processed - 1" in {
    val executor = Executors.newFixedThreadPool(2)
    implicit val ec = ExecutionContext.fromExecutor(executor)

    val latch = new CountDownLatch(1)
    val callbackLatch = new CountDownLatch(1)
    val idq = new IdQueue[Int]()
    // Calculation 1 starts first but calculation 2 will terminate first
    val calc1 = simulateCalculation(idq, id = 1, latch)
    val calc2 = simulateCalculationWithCallback(idq, id = 1) {
      callbackLatch.countDown();
      ()
    }

    // Waiting a few seconds and making sure calc2 is NOT completed
    Thread.sleep(3.seconds.toMillis)
    assert(!calc1.isCompleted)
    assert(!calc2.isCompleted)
    assert(idq.isBusy(id = 1))
    assert(callbackLatch.getCount == 1)
    // Allowing Calculation 1 to terminate
    latch.countDown()
    // Calculation 2 will be unlock
    callbackLatch.await(3, TimeUnit.SECONDS)
    assert(callbackLatch.getCount == 0)
    // Both calculation are finished
    Thread.sleep(1.second.toMillis)
    assert(calc1.isCompleted)
    assert(calc2.isCompleted)

    // Cleanup
    executor.shutdownNow()
  }

  "IdQueue.acquire" should "force a calculation to wait if its id is already being processed - 2" in {
    val executor = Executors.newFixedThreadPool(2)
    implicit val ec = ExecutionContext.fromExecutor(executor)

    val latch1 = new CountDownLatch(1)
    val latch2 = new CountDownLatch(1)
    val latch3 = new CountDownLatch(1)

    val idq = new IdQueue[Int]()
    // Calculation 1 starts first but calculation 2 will terminate first
    val calc1 = simulateCalculation(idq, id = 1, latch1)
    val calc2 = simulateCalculation(idq, id = 1, latch2)
    val calc3 = simulateCalculation(idq, id = 1, latch3)

    // Waiting a few seconds and making sure calc2 is NOT completed
    Thread.sleep(3.seconds.toMillis)
    assert(!calc1.isCompleted)
    assert(!calc2.isCompleted)
    assert(!calc3.isCompleted)
    assert(idq.isBusy(id = 1))
    // Allowing Calculation 1 to terminate
    latch1.countDown()
    Thread.sleep(1.second.toMillis)
    assert(calc1.isCompleted)
    assert(!calc2.isCompleted)
    assert(!calc3.isCompleted)
    // Allowing Calculation 2 to terminate
    latch2.countDown()
    Thread.sleep(1.second.toMillis)
    assert(calc1.isCompleted)
    assert(calc2.isCompleted)
    assert(!calc3.isCompleted)
    // Calculation 3 will be unlock
    latch3.countDown()
    // All calculations are finished
    Thread.sleep(1.second.toMillis)
    assert(calc1.isCompleted)
    assert(calc2.isCompleted)
    assert(calc3.isCompleted)

    // Cleanup
    executor.shutdownNow()
  }

  "IdQueue.acquire" should "force a calculation to wait if its id is already being processed - 3" in {
    val executor = Executors.newFixedThreadPool(8)
    implicit val ec = ExecutionContext.fromExecutor(executor)

    val latch = new CountDownLatch(1)
    val idq = new IdQueue[Int]()
    val list = new ConcurrentLinkedQueue[Int]()
    (1 to 100).foreach { i =>
      simulateCalculationWithCallback(idq, id = 1) {
        Thread.sleep(50)
        list.add(i)
      }
    }
    simulateCalculationWithCallback(idq, id = 1) {
      latch.countDown()
    }
    // 10 seconds should be enough to process all the calculation sequentially
    latch.await(10, TimeUnit.SECONDS)
    assert(latch.getCount == 0)

    // Checking all elements were added in order despite multiple Threads being available
    assert(list.toArray.toList === (1 to 100).toList)

    // Cleanup
    executor.shutdownNow()
  }

  // Helpers

  private[this] def doSomeCallToService[T](value: T, delay: Duration = 1.second)(implicit ec: ExecutionContext): Future[T] =
    Future {
      Thread.sleep(delay.toMillis); value
    }

  private[this] def doSleepUntilReleased(latch: CountDownLatch)(implicit ec: ExecutionContext): Future[Unit] =
    Future {
      latch.await()
    }

  private[this] def simulateCalculation[T](idq: IdQueue[T], id: T)(implicit ec: ExecutionContext): Future[Unit] = {
    val res = for {
      _ <- idq.acquire(id)
      _ <- doSomeCallToService(value = 2)
    } yield ()
    res.onComplete { case _ => idq.release(id) }
    res
  }

  private[this] def simulateCalculation[T](idq: IdQueue[T], id: T, latch: CountDownLatch)(implicit ec: ExecutionContext): Future[Unit] = {
    val res = for {
      _ <- idq.acquire(id)
      _ <- doSleepUntilReleased(latch)
    } yield ()
    res.onComplete { case _ =>
      idq.release(id)
    }
    res
  }

  private[this] def simulateCalculationWithCallback[T](idq: IdQueue[T], id: T)(callback: => Unit)(implicit ec: ExecutionContext): Future[Unit] = {
    val res = for {
      _ <- idq.acquire(id)
    } yield callback
    res.onComplete { case _ =>
      idq.release(id)
    }
    res
  }

}
