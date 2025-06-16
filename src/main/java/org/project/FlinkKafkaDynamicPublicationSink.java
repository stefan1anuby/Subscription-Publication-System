package org.project;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.project.PublicationOuterClass.Publication;

import java.util.Properties;

public class FlinkKafkaDynamicPublicationSink implements SinkFunction<Tuple2<String, Publication>> {

    private transient KafkaProducer<String, String> producer;

    @Override
    public void invoke(Tuple2<String, Publication> value, Context context) throws Exception {
        if (producer == null) {
            Properties props = new Properties();
            props.put("bootstrap.servers", "kafka:29092");
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producer = new KafkaProducer<>(props);
        }

        String topic = value.f0;                  // topic_id
        Publication publication = value.f1;       // protobuf

        String  serialized = publication.toString();  // protobuf serialization

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, serialized);
        producer.send(record);
    }

    public void close() {
        if (producer != null) {
            producer.close();
        }
    }
}
