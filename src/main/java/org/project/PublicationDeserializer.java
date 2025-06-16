package org.project;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class PublicationDeserializer implements DeserializationSchema<PublicationOuterClass.Publication> {
    @Override
    public PublicationOuterClass.Publication deserialize(byte[] bytes) throws IOException {
        return PublicationOuterClass.Publication.parseFrom(bytes);
    }

    @Override
    public boolean isEndOfStream(PublicationOuterClass.Publication nextElement) {
        return false;
    }

    @Override
    public TypeInformation<PublicationOuterClass.Publication> getProducedType() {
        return TypeInformation.of(PublicationOuterClass.Publication.class);
    }
}
