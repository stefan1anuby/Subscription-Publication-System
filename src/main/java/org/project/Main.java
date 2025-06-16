package org.project;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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

        for (int i = 0; i < 10; i++) {
            PublicationOuterClass.Publication value = Generator.generatePublication();

            //String value = "test";
            producer.send(new ProducerRecord<>("input-topic", value));
            System.out.println("Sent record1: " + value.toString());
            //Thread.sleep(500); // simulate streaming
        }

        producer.close();
    }

    public static void run_flick_job() throws Exception {

        System.out.println("run_flick_job STARTED !!!!!!");
        // 1. Set up the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.out.println("run_flick_job got exec env !!!!!!");
        // 2. Kafka consumer properties
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "kafka:29092");
        kafkaProps.setProperty("group.id", "flink-group");

        System.out.println("run_flick_job created kafka consumer proprietes !!!!!!");
        // 3. Create Kafka consumer
        FlinkKafkaConsumer<PublicationOuterClass.Publication> consumer = new FlinkKafkaConsumer<>(
                "input-topic",
                new PublicationDeserializer(),
                kafkaProps
        );

        // 4. Create Kafka producer
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                "output-topic",
                new SimpleStringSchema(),
                kafkaProps
        );

        System.out.println("run_flick_job created kafka consumer and producer !!!!!!");

        // 5. Define the Flink pipeline

        DataStreamSource<PublicationOuterClass.Publication> input = env.addSource(consumer);

        DataStream<String> transformed = input
                .filter(Objects::nonNull)
                .map(publication -> "Received publication: " + publication.getStationId());

        transformed.addSink(producer);

        System.out.println("run_flick_job executing the job !!!!!!");
        // 6. Run the job
        env.execute("Flink Kafka Streaming Job");
    }
    public static void main(String[] args) throws Exception {

        //run_producer();
        run_flick_job();
    }
}