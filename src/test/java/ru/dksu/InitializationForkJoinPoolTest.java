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
    }
}