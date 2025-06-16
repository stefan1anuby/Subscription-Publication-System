package org.project;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class SubscriptionDeserializer implements DeserializationSchema<SubscriptionWrapper> {

    @Override
    public SubscriptionWrapper deserialize(byte[] message) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(message, SubscriptionWrapper.class);
    }

    @Override
    public boolean isEndOfStream(SubscriptionWrapper nextElement) {
        return false;
    }

    @Override
    public TypeInformation<SubscriptionWrapper> getProducedType() {
        return TypeInformation.of(SubscriptionWrapper.class);
    }
}
