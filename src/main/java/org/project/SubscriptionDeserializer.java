package org.project;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SubscriptionDeserializer implements DeserializationSchema<SubscriptionWrapper> {

    @Override
    public SubscriptionWrapper deserialize(byte[] message) throws IOException {
        String raw = new String(message, StandardCharsets.UTF_8).trim();

        // Extract topic_id manually
        // Format: subscription-abc123##{(city,=,"Bucharest");(temp,>=,10)}
        String[] parts = raw.split("##");
        if (parts.length != 2) throw new IOException("Invalid format: " + raw);

        String topicId = parts[0].trim();
        String conditionString = parts[1].trim();

        Subscription sub = new Subscription();

        Pattern pattern = Pattern.compile("\\(([^,]+),([^,]+),\"?([^)\"]+)\"?\\)");
        Matcher matcher = pattern.matcher(conditionString);
        while (matcher.find()) {
            String field = matcher.group(1).trim();
            String operator = matcher.group(2).trim();
            String value = matcher.group(3).trim();
            sub.addCondition(field, operator, value);
        }

        SubscriptionWrapper wrapper = new SubscriptionWrapper();
        wrapper.setTopicId(topicId);
        wrapper.setSubscription(sub);

        System.out.println("deserialize !!!!!!!!!! am deserializat " + wrapper.toString());

        return wrapper;
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
