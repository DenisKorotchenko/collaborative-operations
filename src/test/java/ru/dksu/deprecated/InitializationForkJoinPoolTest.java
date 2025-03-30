package ru.dksu.deprecated;

import org.junit.jupiter.api.Test;

class InitializationForkJoinPoolTest {
    @Test
    void test() {
        var array = new InitializationForkJoinPool<>();
        array.start(0);
        array.start(1);
        int n = 20;
        long sum = 0;
        for (int i = 0; i < n; i++) {
            sum += array.start(i);
        }
        System.out.println("Mean time: " + sum / n);
    }
}