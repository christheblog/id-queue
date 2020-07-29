# IdQueue 
An IdQueue is a queue ensuring a given id cannot be processed concurrently, 
while allowing concurrent processing of different ids.

# Usage
```scala
implicit val ec: ExecutionContext = ...

val idq = new IdQueue[Int]()

val myFut = for {
  _ <- idq.acquire("id-1")
  value <- retrieveFromService("id-1", ...)
  success <- writeToDatabase("id-1", value, ...)
} yield success

myFut.onComplete { case _ => idq.release("id-1") }
```

```IdQueue.acquire``` is returning a Future[Unit] which will be completed once the previous processing on the id is finished.

```IdQueue.release``` needs to be called once teh processing is over, successful or not. 
This allow teh next calculation on the id to proceed.

All calls to ```IdQueue.acquire``` will be processed in order for a given id. Different ids can be processed in parallel.