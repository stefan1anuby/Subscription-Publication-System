package org.project;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.project.PublicationOuterClass.Publication;

import java.util.Map;

public class PublicationSubscriptionMatcher
        extends BroadcastProcessFunction<Publication, SubscriptionWrapper, String> {

    private final MapStateDescriptor<String, Subscription> descriptor =
            new MapStateDescriptor<>("subscriptions", String.class, Subscription.class);

    @Override
    public void processBroadcastElement(
            SubscriptionWrapper wrapper,
            Context ctx,
            Collector<String> out) throws Exception {

        String topicId = wrapper.getTopicId();
        Subscription subscription = wrapper.getSubscription();

        if (topicId != null && subscription != null) {
            ctx.getBroadcastState(descriptor).put(topicId, subscription);
            System.out.println("[INFO] Added subscription with topic_id: " + topicId);
        } else {
            System.err.println("[WARN] Skipped invalid subscription wrapper: " + wrapper);
        }
    }

    @Override
    public void processElement(
            Publication publication,
            ReadOnlyContext ctx,
            Collector<String> out) throws Exception {

        ReadOnlyBroadcastState<String, Subscription> subscriptions =
                ctx.getBroadcastState(descriptor);

        for (Map.Entry<String, Subscription> entry : subscriptions.immutableEntries()) {
            String topicId = entry.getKey();
            Subscription subscription = entry.getValue();

            if (Matcher.matches(publication, subscription)) {
                String message = String.format("Matched topic_id: %s -> %s", topicId, publication.toString());
                out.collect(message); // This is what goes to Kafka or print
                System.out.println("[MATCH] " + message);
            }
        }
    }
}
