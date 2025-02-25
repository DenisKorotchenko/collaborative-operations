package ru.dksu;

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
}