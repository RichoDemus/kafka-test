
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.LongDeserializer
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.Properties


val logger = LoggerFactory.getLogger("Main")!!


internal fun props() : Properties {
    val props = Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            LongSerializer::class.java.name)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            LongDeserializer::class.java.name)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer::class.java.name)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            StringSerializer::class.java.name)
    // props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString())
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "richo-group-4")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    // props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    return props
}



fun main(args: Array<String>) {
    listOf("Hello", "world").joinToString(" ").let { println(it) }




    consume()

    // produce()
}

private fun produce() {
    val producer = KafkaProducer<Any, Any>(props())


    val time = System.currentTimeMillis()
    val sendMessageCount = 10000

    try {
        for (index in time until time + sendMessageCount) {
            val record: ProducerRecord<Any, Any> = ProducerRecord("test", index, "Hello Mom " + index)

            val metadata = producer.send(record).get()

            val elapsedTime = System.currentTimeMillis() - time
            System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n",
                    record.key(), record.value(), metadata.partition(),
                    metadata.offset(), elapsedTime)

        }
    } finally {
        producer.flush()
        producer.close()
    }
}

private fun consume() {
    val consumer: Consumer<String, String> = KafkaConsumer(props())

    // Subscribe to the topic.
    consumer.subscribe(listOf("test"))

    val records = consumer.poll(5000)

    logger.info("Received {} records...", records.count())

    records.forEach { record ->
        System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                record.key(), record.value(),
                record.partition(), record.offset())
    }
}
