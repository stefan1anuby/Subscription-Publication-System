package org.project;

public class SubscriptionWrapper {
    private String topic_id;
    private Subscription subscription;

    public String getTopicId() {
        return topic_id;
    }

    public Subscription getSubscription() {
        return subscription;
    }

    public void setTopicId(String topic_id) {
        this.topic_id = topic_id;
    }

    public void setSubscription(Subscription subscription) {
        this.subscription = subscription;
    }
}
