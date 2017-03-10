package com.james.jdk8;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Eight {
    public static void main(String[] args) {
        // stream()
        List<Integer> numbers = Arrays.asList(5, 3, 9, 4);

        numbers.stream().sorted();
        for (int number : numbers) {
            System.out.print(number + " ");
        }
        System.out.println("\n");

        numbers.stream().sorted().forEach((n) -> System.out.print(n + " "));
        System.out.println("\n");

        numbers.stream().sorted().map(n -> n * 2).forEach(n -> System.out.print(n + " "));
        System.out.println("\n");

        numbers.stream().filter(n -> n % 2 == 0).forEach(System.out::print);
        System.out.println("\n");

        long result = numbers.stream().filter(n -> n % 2 == 0).count();
        System.out.println(result + "\n");

        // // parallelStream()
        // int max = 1000000;
        // List<String> values = new ArrayList<>(max);
        // for (int i = 0; i < max; i++) {
        // UUID uuid = UUID.randomUUID();
        // values.add(uuid.toString());
        // }
        //
        // long t00 = System.nanoTime();
        // long count = values.stream().sorted().count();
        // System.out.println(count);
        // long t01 = System.nanoTime();
        // long millis = TimeUnit.NANOSECONDS.toMillis(t01 - t00);
        // System.out.println(String.format("sequential sort took: %d ms",
        // millis));
        //
        // long t10 = System.nanoTime();
        // long count1 = values.parallelStream().sorted().count();
        // System.out.println(count1);
        // long t11 = System.nanoTime();
        // long millis1 = TimeUnit.NANOSECONDS.toMillis(t11 - t10);
        // System.out.println(String.format("parallel sort took: %d ms",
        // millis1));
        // System.out.println("\n");

        // Map
        Map<Integer, String> map = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            map.putIfAbsent(i, "val" + i);
        }

        map.forEach((id, val) -> System.out.println(id + " " + val));
        System.out.println("");

        map.computeIfPresent(3, (num, val) -> val + num);
        System.out.println(map.get(3)); // val33
        map.computeIfPresent(9, (num, val) -> null);
        System.out.println(map.containsKey(9)); // false
        map.computeIfAbsent(23, num -> "val" + num);
        System.out.println(map.containsKey(23)); // true
        map.computeIfAbsent(3, num -> "bam");
        System.out.println(map.get(3)); // val33
        System.out.println("\n");
    }
}