package org.project;

import java.util.ArrayList;
import java.util.List;

class Subscription {
    List<SubscriptionCondition> conditions = new ArrayList<>();

    public void addCondition(String field, String operator, String value) {
        conditions.add(new SubscriptionCondition(field, operator, value));
    }

    public List<SubscriptionCondition> getConditions() {
        return conditions;
    }

    @Override
    public String toString() {
        return "{" + String.join(";", conditions.stream().map(Object::toString).toArray(String[]::new)) + "}";
    }
}
