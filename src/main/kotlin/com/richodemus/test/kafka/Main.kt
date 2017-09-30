import com.richodemus.test.kafka.AdditionConsumer
import com.richodemus.test.kafka.InitialProducer
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


fun main(args: Array<String>) {
    val threadPool = Executors.newCachedThreadPool()

    val producer = InitialProducer("test", 10)
    val consumer = AdditionConsumer("test", 10)

    threadPool.execute(consumer)
    threadPool.execute(producer)

    threadPool.shutdown()
    threadPool.awaitTermination(1, TimeUnit.HOURS)
}
