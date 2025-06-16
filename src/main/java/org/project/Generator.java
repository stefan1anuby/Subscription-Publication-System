package org.project;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.util.*;

public class Generator {
    static Random rand = new Random();
    static List<String> cities = Arrays.asList("Bucharest", "Cluj", "Iasi", "Timisoara");
    static List<String> directions = Arrays.asList("N", "NE", "E", "SE", "S", "SW", "W", "NW");
    static List<Integer> stationIds = Arrays.asList(1, 2, 3, 4, 5);

    static Map<String, Integer> fieldStats = new HashMap<>();
    static Map<String, Integer> operatorStats = new HashMap<>();


    public static PublicationOuterClass.Publication generatePublication() {
        return PublicationOuterClass.Publication.newBuilder()
                .setStationId(getRandom(stationIds))
                .setCity(getRandom(cities))
                .setTemp(rand.nextInt(41)) // 0-40
                .setRain(Math.round(rand.nextDouble() * 100) / 10.0) // 0.0 - 10.0
                .setWind(rand.nextInt(31)) // 0-30
                .setDirection(getRandom(directions))
                .setDate(LocalDate.now().minusDays(rand.nextInt(30)).toString())
                .build();
    }

    public static Subscription generateSubscription(int index) {
        Subscription sub = new Subscription();
        int n = Config.numMessages;

        if (index < n * Config.cityFrequency) {
            String city = getRandom(cities);
            String operator = index < n * Config.cityFrequency * Config.cityEqualityOperatorRatio ? "=" : "!=";
            sub.addCondition("city", operator, city);
            increment(fieldStats, "city");
            increment(operatorStats, "city:" + operator);
        }

        if (index < n * Config.tempFrequency) {
            String operator = getRandom(Arrays.asList(">=", "<", "=", ">"));
            int temp = rand.nextInt(41);
            sub.addCondition("temp", operator, String.valueOf(temp));
            increment(fieldStats, "temp");
            increment(operatorStats, "temp:" + operator);
        }

        if (index < n * Config.windFrequency) {
            String operator = getRandom(Arrays.asList("<", ">=", "=", ">"));
            int wind = rand.nextInt(31);
            sub.addCondition("wind", operator, String.valueOf(wind));
            increment(fieldStats, "wind");
            increment(operatorStats, "wind:" + operator);
        }

        //... maybe add some more conditions here?
        //... regarding the statistics, how about just printing the subriptions to the output and just CTRL F on logs and use regex manually to make a statistics for a specific type of subription

        return sub;
    }

    public static void increment(Map<String, Integer> map, String key) {
        map.put(key, map.getOrDefault(key, 0) + 1);
    }

    public static <T> T getRandom(List<T> list) {
        return list.get(rand.nextInt(list.size()));
    }

    public static void writeToFile(String filename, List<String> lines) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            for (String line : lines) {
                writer.write(line);
                writer.newLine();
            }
        }
    }

    public static void printStats() {
        System.out.println("\nSubscription Field Stats:");
        fieldStats.forEach((k, v) -> System.out.printf("%s: %d\n", k, v));

        System.out.println("\nSubscription Operator Stats:");
        operatorStats.forEach((k, v) -> System.out.printf("%s: %d\n", k, v));
    }
}
