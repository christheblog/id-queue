package org.cc.idq

import java.util.concurrent.{ConcurrentLinkedDeque, ConcurrentLinkedQueue}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{Future, Promise}

object IdQueue {
  type Q[A] = ConcurrentLinkedDeque[A]
}

class IdQueue[Id] {

  import IdQueue.Q

  // Queue per id
  private[this] val ids = new TrieMap[Id, Q[Promise[Unit]]]()

  def acquire(id: Id): Future[Unit] = {
    ids.putIfAbsent(id, new Q())
    val q = ids(id)
    // Peeking the last item of the queue,
    // It will be the future we need to wait on.
    // And then adding a promise in the queue - which will be filled when release will be called
    // These 2 operations need to be done atomically, hence the synchronization
    q.synchronized {
      val peeked = Option(q.peekLast())
      q.add(Promise())
      peeked.map(_.future)
        .getOrElse(Future.successful(()))
    }
  }

  // Note:
  // We don't remove the queue from the map when it is empty
  // This would require "locking" on the ids map to ensure nobody can start acquiring the queue to add a promise
  def release(id: Id): Unit = {
    ids.get(id).foreach { q =>
      q.synchronized {
        Option(q.poll()).foreach(_.success(()))
      }
    }
  }

  // To use release in a for loop (dangerous in case a previous future is failing ...
  // Maybe this should be removed
  def liftRelease(id: Id): Future[Unit] =
    Future.successful(release(id))

  def isBusy(id: Id): Boolean =
    ids.get(id).exists(!_.isEmpty)

  def isEmpty(id: Id): Boolean =
    ids.get(id).forall(_.isEmpty)

  def size(id: Id): Int =
    ids.get(id).map(_.size).getOrElse(0)

  def isEmpty(): Boolean =
    ids.isEmpty

  def size(): Int =
    ids.size
}
