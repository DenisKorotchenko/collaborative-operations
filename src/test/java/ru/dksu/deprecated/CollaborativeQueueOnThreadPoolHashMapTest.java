package ru.dksu.deprecated;

import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.Random;

class CollaborativeQueueOnThreadPoolHashMapTest {
    @Test
    void mainTest() throws InterruptedException {
        Random r = new Random();
        r.setSeed(239);
        int elems = 500000;
        int iters = 10;
        int threads = 1;

        for (int q = 0; q < iters; q++) {
            var collaborativeHashMap = new CollaborativeQueueOnThreadPoolHashMap<>(16);
//            var baseHashMap = new HashMap<Integer, Integer>();
            LinkedList<Thread> runs = new LinkedList<>();
            for (int i = 0; i < threads; i++) {
                var thread = new Thread() {
                    @Override
                    public void run() {
                        System.out.println("Start thread " + Thread.currentThread().getName());
                        Random localR = new Random();
                        int operations = 0;
                        localR.setSeed(r.nextInt());
                        for (int i = 0; i < elems / threads; i++) {
                            var key = localR.nextInt();
                            var value = localR.nextInt();
                            collaborativeHashMap.put(key, value);
                            operations++;
                            key = localR.nextInt();
                            collaborativeHashMap.get(key);
                        }
                        System.out.println("Finish thread " + Thread.currentThread().getName() + " Operations count: " + operations);
                    }
                };
                runs.add(thread);
            }
            for (var thread: runs) {
                thread.start();
            }
            for (var thread: runs) {
                thread.join();
            }
            System.out.println("Size:  " + collaborativeHashMap.size());
            System.out.println("Size2: " + collaborativeHashMap.size2());
//            for (int i = 0; i < elems; i++) {
//                var key = r.nextInt();
//                var value = r.nextInt();
//                collaborativeHashMap.put(key, value);
//                baseHashMap.put(key, value);
//                Assertions.assertEquals(baseHashMap.get(key), collaborativeHashMap.get(key));
//            }
//            Assertions.assertEquals(baseHashMap.size(), collaborativeHashMap.size());
        }
        var x = CollaborativeQueueOnThreadPoolHashMap.rebuildTimes;
        x = x;
        var keys = x.keySet().stream().sorted().toArray();
        var key = keys[keys.length-1];
        System.out.println("Buckets size: " + key);
        var values = x.get(key).stream().sorted().toList().subList(1, iters - 1);
        Long valuesSum = 0L;
        for (Long value: values) {
            valuesSum += value;
        }
        Long meanValue = valuesSum / values.size();
        System.out.println("Mean rebuild time: " + meanValue);
    }
}