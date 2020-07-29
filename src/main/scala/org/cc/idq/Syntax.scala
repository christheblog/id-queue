package org.cc.idq

import scala.concurrent.{ExecutionContext, Future}

object Syntax {

  // Provides a function call that preform the acquire/release safely
  def acquire[Id,T](idq: IdQueue[Id])
                   (id: Id)
                   (body: Id => Future[T])
                   (implicit ec: ExecutionContext): Future[T] = {
    val fut = for {
      _ <- idq.acquire(id)
      res <- body(id)
    } yield res
    fut.onComplete { case _ => idq.release(id) }
    fut
  }

}
