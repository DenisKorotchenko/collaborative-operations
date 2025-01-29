package ru.dksu;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Random;

class InitializationForkJoinPoolTest {
    @Test
    void test() {
        var array = new InitializationForkJoinPool<>();
        array.start();
        array.start();
        int n = 20;
        long sum = 0;
        for (int i = 0; i < n; i++) {
            sum += array.start();
        }
        System.out.println("Mean time: " + sum / n);
    }
}