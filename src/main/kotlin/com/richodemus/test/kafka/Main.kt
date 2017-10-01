import com.richodemus.test.kafka.AdditionConsumer
import com.richodemus.test.kafka.InitialProducer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

val logger: Logger = LoggerFactory.getLogger("Main")

fun main(args: Array<String>) {
    logger.info("Starting...")
    val threadPool = Executors.newCachedThreadPool()

    val messages = 10
    val topic = "test"
    val producer = InitialProducer(topic, messages)
    val consumer = AdditionConsumer(topic, messages)


    Thread.sleep(1000L)
    logger.info("Executing producer...")
    threadPool.execute(producer)

    threadPool.shutdown()
    threadPool.awaitTermination(1, TimeUnit.HOURS)
}
