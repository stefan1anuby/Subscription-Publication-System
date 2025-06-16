package org.project;

import org.apache.kafka.common.serialization.Serializer;

public class PublicationSerializer implements Serializer<PublicationOuterClass.Publication> {
    @Override
    public byte[] serialize(String topic, PublicationOuterClass.Publication data) {
        return data.toByteArray(); // Protobuf handles it
    }
}
