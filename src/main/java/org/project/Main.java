package org.project;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Objects;
import java.util.Properties;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.project.Generator;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {

    public static void run_producer() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", PublicationSerializer.class.getName());

        KafkaProducer<String, PublicationOuterClass.Publication> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 500; i++) {
            PublicationOuterClass.Publication value = Generator.generatePublication();

            //String value = "test";
            producer.send(new ProducerRecord<>("input-topic", value));
            System.out.println("Sent record1: " + value.toString());
            //Thread.sleep(500); // simulate streaming
        }

        /// TEST
        /*
        // Step 1: Define and parse the subscription from string
        String input = "subscription-abc123##{(city,=,\"Iasi\");(temp,>=,10)}";
        SubscriptionDeserializer deserializer = new SubscriptionDeserializer();
        SubscriptionWrapper wrapper = deserializer.deserialize(input.getBytes());

        Subscription subscription = wrapper.getSubscription();
        System.out.println("Parsed Subscription: " + subscription);
        System.out.println("Using topic ID: " + wrapper.getTopicId());
        System.out.println();

        // Step 2: Generate and test publications
        for (int i = 0; i < 100; i++) {
            PublicationOuterClass.Publication pub = Generator.generatePublication();
            boolean match = Matcher.matches(pub, subscription);

            System.out.println("Generated publication: " + pub);
            System.out.println("  → Match: " + match);
            System.out.println();
        }

        System.out.println("✅ Finished testing matcher.");
        */
        producer.close();
    }

    public static void run_flick_job() throws Exception {
        System.out.println("run_flick_job STARTED");
        //System.out.println(new com.fasterxml.jackson.databind.ObjectMapper());

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka configuration
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "kafka:29092");
        kafkaProps.setProperty("group.id", "flink-group");

        // Publication consumer (Protobuf)
        FlinkKafkaConsumer<PublicationOuterClass.Publication> publicationConsumer = new FlinkKafkaConsumer<>(
                "input-topic",
                new PublicationDeserializer(),
                kafkaProps
        );

        // Subscription consumer (JSON with topic_id)
        FlinkKafkaConsumer<SubscriptionWrapper> subscriptionConsumer = new FlinkKafkaConsumer<>(
                "subscriptions",
                new SubscriptionDeserializer(),
                kafkaProps
        );

        // Broadcast subscription stream
        MapStateDescriptor<String, Subscription> descriptor =
                new MapStateDescriptor<>("subscriptions", String.class, Subscription.class);

        BroadcastStream<SubscriptionWrapper> subscriptionBroadcastStream =
                env.addSource(subscriptionConsumer).broadcast(descriptor);

        // Read publication stream
        DataStream<PublicationOuterClass.Publication> publicationStream = env.addSource(publicationConsumer)
                .filter(Objects::nonNull);

        // Process publications against subscriptions
        DataStream<Tuple2<String, PublicationOuterClass.Publication>> matchedStream = publicationStream
                .connect(subscriptionBroadcastStream)
                .process(new PublicationSubscriptionMatcher());

        // Output matched messages to a Kafka topic
        FlinkKafkaProducer<String> matchedProducer = new FlinkKafkaProducer<>(
                "matched-publications",
                new SimpleStringSchema(),
                kafkaProps
        );

        matchedStream.addSink(new FlinkKafkaDynamicPublicationSink());

        System.out.println("Executing Flink job...");
        env.execute("Publication Filtering Job");
    }

    public static void main(String[] args) throws Exception {

        run_producer();
        //run_flick_job();
    }
}

// TO READ FROM KAFKA TOPIC (intrand in kafka docker container)
// kafka-console-consumer  --bootstrap-server kafka:29092  --topic input-topic  --from-beginning