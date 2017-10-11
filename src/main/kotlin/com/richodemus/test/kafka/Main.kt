
import com.richodemus.test.kafka.AdditionConsumer
import com.richodemus.test.kafka.InitialProducer
import com.richodemus.test.kafka.NonProducingConsumer
import com.richodemus.test.kafka.Stream
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

val logger: Logger = LoggerFactory.getLogger("Main")

fun main(args: Array<String>) {
    logger.info("Starting...")
    val threadPool = Executors.newCachedThreadPool()


    val stream = Stream("C", "D")


    val messages = 100
    val producer = InitialProducer("A", 1)
    val workers = listOf(
            Pair("A", "B"),
            Pair("B", "C"),
            //Pair("C", "D"),
            Pair("D", "E"),
            Pair("E", "F"),
            Pair("F", "G")
    )
            .map { AdditionConsumer("${it.first}->${it.second}", it.first, it.second) }

    val consumer = NonProducingConsumer("G", messages)


    Thread.sleep(3000L)
    logger.info("Executing producer...")
    threadPool.execute(producer)

    while (consumer.running) {
        Thread.sleep(10L)
    }

    workers.forEach(AdditionConsumer::stop)

    threadPool.shutdown()
    threadPool.awaitTermination(1, TimeUnit.HOURS)
    stream.close()
}
