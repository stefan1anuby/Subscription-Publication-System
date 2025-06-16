package org.project;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.project.PublicationOuterClass.Publication;

import java.util.Map;

public class PublicationSubscriptionMatcher
        extends BroadcastProcessFunction<Publication, SubscriptionWrapper, Tuple2<String, Publication>> {

    private final MapStateDescriptor<String, Subscription> descriptor =
            new MapStateDescriptor<>("subscriptions", String.class, Subscription.class);

    @Override
    public void processBroadcastElement(
            SubscriptionWrapper wrapper,
            Context ctx,
            Collector<Tuple2<String, Publication>> out) throws Exception {

        String topicId = wrapper.getTopicId();
        Subscription subscription = wrapper.getSubscription();

        if (topicId != null && subscription != null) {
            ctx.getBroadcastState(descriptor).put(topicId, subscription);
        }
    }

    @Override
    public void processElement(
            Publication publication,
            ReadOnlyContext ctx,
            Collector<Tuple2<String, Publication>> out) throws Exception {

        ReadOnlyBroadcastState<String, Subscription> subscriptions =
                ctx.getBroadcastState(descriptor);

        for (Map.Entry<String, Subscription> entry : subscriptions.immutableEntries()) {
            String topicId = entry.getKey();
            Subscription subscription = entry.getValue();

            if (Matcher.matches(publication, subscription)) {
                out.collect(new Tuple2<>(topicId, publication));
                System.out.println("!!!!!!!!! processElement Matched: " + topicId + " " + publication.toString());
            }
            else {
                System.out.println("!!!!!!!!!  processElement NOT Matched: " + topicId + " " + publication.toString());
            }
        }
    }
}
