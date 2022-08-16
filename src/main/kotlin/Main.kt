import io.confluent.parallelconsumer.ParallelConsumerOptions
import io.confluent.parallelconsumer.ParallelStreamProcessor
import io.confluent.parallelconsumer.PollContext
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStream
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles
import java.util.*

private val logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())

fun main() {

    // CREATE KS TOPIC
    val adminClient = AdminClient.create(Properties().apply {
        put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    })
    adminClient.createIfNotExist(NewTopic("foo", 1, 1))

    // CREATE TOPOLOGY
    val streamsBuilder = StreamsBuilder()
    streamsBuilder.stream<String, String>("foo")
        .peek { key, value -> logger.info("KS input key=$key value=$value in kafka-streams") }
        .parallelTransform(
            sb = streamsBuilder,
            ac = adminClient,
            name = "foo-parallel-transform",
            threads = 64
        ) {
            Thread.sleep(2000) // expensive transformation...
            KeyValue(it.key(), "${it.value()}-transformed")
        }
        .peek { key, value -> logger.info("KS transformed key=$key value=$value") }

    val topology = streamsBuilder.build()
    logger.info(topology.describe().toString())

    // RUN KAFKA STREAMS
    val kafkaStreams = KafkaStreams(topology, Properties().apply {
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        put(StreamsConfig.APPLICATION_ID_CONFIG, "my-ks-app")
        put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()::class.java)
        put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String()::class.java)
    })

    kafkaStreams.start()
    Runtime.getRuntime().addShutdownHook(Thread(kafkaStreams::close))
}

// STUPID IMPLEMENTATION OF KS EXTENSION FUNCTION
fun KStream<String, String>.parallelTransform(
    sb: StreamsBuilder,
    ac: AdminClient,
    name: String,
    threads: Int,
    transformer: (keyValue: PollContext<String, String>) -> KeyValue<String, String>,
): KStream<String, String> {
    val pcInputTopic = "$name-pc-in"
    val pcOutputTopic = "$name-pc-out"

    ac.createIfNotExist(
        NewTopic(pcInputTopic, 1, 1),
        NewTopic(pcOutputTopic, 1, 1),
    )

    // forward to pc input
    this.to(pcInputTopic)

    // set up and start PC
    ParallelStreamProcessor.createEosStreamProcessor(
        ParallelConsumerOptions.builder<String, String>()
            .maxConcurrency(threads)
            .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
            .consumer(
                KafkaConsumer(
                    Properties().apply {
                        put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
                        put(ConsumerConfig.GROUP_ID_CONFIG, "$name-pc")
                        put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
                    },
                    // should be derived from type somehow
                    Serdes.String().deserializer(),
                    Serdes.String().deserializer()
                )
            )
            .producer(
                KafkaProducer(
                    Properties().apply { put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092") },
                    // should be derived from type somehow
                    Serdes.String().serializer(),
                    Serdes.String().serializer()
                )
            )
            .build()
    ).run {
        subscribe(listOf(pcInputTopic))
        pollAndProduce {
            logger.info("PC transforming key=${it.key()} value=${it.value()}")
            val transformed = transformer(it)
            ProducerRecord(pcOutputTopic, transformed.key, transformed.value)
        }
        Runtime.getRuntime().addShutdownHook(Thread(::close))
    }

    // stream pc output
    return sb.stream(pcOutputTopic)
}

fun AdminClient.createIfNotExist(vararg topics: NewTopic) {
    kotlin.runCatching {
        this.createTopics(topics.toList()).all().get()
    }.onFailure {
        if (it.cause is TopicExistsException) {
            logger.debug(it.message)
        } else {
            throw it
        }
    }
}