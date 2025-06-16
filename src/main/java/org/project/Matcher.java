package org.project;

import java.lang.reflect.Field;
import java.time.LocalDate;

public class Matcher {
    public static boolean matches(PublicationOuterClass.Publication publication, Subscription subscription) {
        for (SubscriptionCondition condition : subscription.getConditions()) {
            String fieldName = condition.field;
            String operator = condition.operator;
            String expectedValue = condition.value;

            Object actualValue;

            // Manual mapping from field name → actual value
            switch (fieldName) {
                case "stationId":
                    actualValue = publication.getStationId();
                    break;
                case "city":
                    actualValue = publication.getCity();
                    break;
                case "temp":
                    actualValue = publication.getTemp();
                    break;
                case "rain":
                    actualValue = publication.getRain();
                    break;
                case "wind":
                    actualValue = publication.getWind();
                    break;
                case "direction":
                    actualValue = publication.getDirection();
                    break;
                default:
                    System.err.println("Unknown field in condition: " + fieldName);
                    return false;
            }

            if (!evaluate(actualValue, operator, expectedValue)) {
                return false;
            }
        }
        return true;
    }

    private static boolean evaluate(Object actualValue, String operator, String expectedValue) {
        if (actualValue instanceof String) {
            return operator.equals("=") && actualValue.equals(expectedValue);
        }

        if (actualValue instanceof Integer) {
            int actual = (Integer) actualValue;
            int expected = Integer.parseInt(expectedValue);
            return evaluateNumeric(actual, operator, expected);
        }

        if (actualValue instanceof Double) {
            double actual = (Double) actualValue;
            double expected = Double.parseDouble(expectedValue);
            return evaluateNumeric(actual, operator, expected);
        }

        if (actualValue instanceof LocalDate) {
            LocalDate actual = (LocalDate) actualValue;
            LocalDate expected = LocalDate.parse(expectedValue);
            switch (operator) {
                case "=": return actual.equals(expected);
                case "<": return actual.isBefore(expected);
                case ">": return actual.isAfter(expected);
                case "<=": return actual.isBefore(expected) || actual.equals(expected);
                case ">=": return actual.isAfter(expected) || actual.equals(expected);
                default: return false;
            }
        }

        return false;
    }

    private static <T extends Comparable<T>> boolean evaluateNumeric(T actual, String operator, T expected) {
        switch (operator) {
            case "=": return actual.compareTo(expected) == 0;
            case "<": return actual.compareTo(expected) < 0;
            case ">": return actual.compareTo(expected) > 0;
            case "<=": return actual.compareTo(expected) <= 0;
            case ">=": return actual.compareTo(expected) >= 0;
            default: return false;
        }
    }
}
