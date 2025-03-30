package ru.dksu.deprecated;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Random;

class CollaborativeThreadPoolHashMapTest {
    @Test
    void size() {
        Random r = new Random();
        r.setSeed(239);
        int elems = 1000000;
        int iters = 10;


        for (int q = 0; q < iters; q++) {
            var collaborativeHashMap = new CollaborativeThreadPoolHashMap<Integer, Integer>();
            var baseHashMap = new HashMap<Integer, Integer>();
            for (int i = 0; i < elems; i++) {
                var key = r.nextInt();
                var value = r.nextInt();
                collaborativeHashMap.put(key, value);
                baseHashMap.put(key, value);
                Assertions.assertEquals(baseHashMap.get(key), collaborativeHashMap.get(key));
            }
            Assertions.assertEquals(baseHashMap.size(), collaborativeHashMap.size());
        }
        var x = CollaborativeThreadPoolHashMap.rebuildTimes;
        x = x;
        var keys = x.keySet().stream().sorted().toArray();
        var key = keys[keys.length-1];
        System.out.println("Buckets size: " + key);
        var values = x.get(key).stream().sorted().toList().subList(2, iters - 2);
        Long valuesSum = 0L;
        for (Long value: values) {
            valuesSum += value;
        }
        Long meanValue = valuesSum / values.size();
        System.out.println("Mean rebuild time: " + meanValue);
    }
}