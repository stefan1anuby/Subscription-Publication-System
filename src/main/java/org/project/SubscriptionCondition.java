package org.project;

class SubscriptionCondition {
    String field;
    String operator;
    String value;

    public SubscriptionCondition(String field, String operator, String value) {
        this.field = field;
        this.operator = operator;
        this.value = value;
    }

    @Override
    public String toString() {
        return String.format("(%s,%s,\"%s\")", field, operator, value);
    }
}
