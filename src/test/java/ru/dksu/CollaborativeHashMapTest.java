package ru.dksu;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

class CollaborativeHashMapTest {



    @Test
    void size() {
        var collaborativeHashMap = new CollaborativeHashMap<Integer, Integer>();
        var baseHashMap = new HashMap<Integer, Integer>();
        for (int i = 0; i < 25600; i++) {
            collaborativeHashMap.put(i, i);
            baseHashMap.put(i, i);
            Assertions.assertEquals(baseHashMap.get(i), collaborativeHashMap.get(i));
        }
        Assertions.assertEquals(25600, collaborativeHashMap.size());
    }
}