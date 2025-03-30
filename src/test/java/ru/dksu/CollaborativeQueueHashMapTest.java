package ru.dksu;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

class CollaborativeQueueHashMapTest {
    @Test
    void mainTest() throws InterruptedException {
        Random r = new Random();
        r.setSeed(239);
        int elems = 500000;
        int iters = 5;
        int threads = 5;

        for (int q = 0; q < iters; q++) {
            var startTime = System.nanoTime();
            var collaborativeHashMap = new CollaborativeQueueHashMap<>(16);
//            var baseHashMap = new HashMap<Integer, Integer>();
            LinkedList<Thread> runs = new LinkedList<>();
            AtomicInteger done = new AtomicInteger(0);
            for (int i = 0; i < threads; i++) {
                var thread = new Thread() {
                    @Override
                    public void run() {
                        System.out.println("Start thread " + Thread.currentThread().getName());
                        Random localR = new Random();
                        int operations = 0;
                        localR.setSeed(r.nextInt());
                        while (done.getAndIncrement() < elems / 1000) {
                            for (int q = 0; q < 1000; q++) {
                                var key = localR.nextInt();
                                var value = localR.nextInt();
                                collaborativeHashMap.put(key, value);
                                operations++;
                                key = localR.nextInt();
                                collaborativeHashMap.get(key);
                            }
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
            System.out.println("Time:  " + (System.nanoTime() - startTime));
//            for (int i = 0; i < elems; i++) {
//                var key = r.nextInt();
//                var value = r.nextInt();
//                collaborativeHashMap.put(key, value);
//                baseHashMap.put(key, value);
//                Assertions.assertEquals(baseHashMap.get(key), collaborativeHashMap.get(key));
//            }
//            Assertions.assertEquals(baseHashMap.size(), collaborativeHashMap.size());
        }
        var x = CollaborativeQueueHashMap.rebuildTimes;
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

    @Disabled
    @Test
    void multithreadTest() throws InterruptedException {
//        AtomicInteger i = new AtomicInteger(0);
//        final int[] i = {0};

        Random random = new Random();
        random.setSeed(System.nanoTime());
        int threadCount = random.nextInt(1, 3);
        System.out.println(threadCount);


        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int j = 0; j < threadCount; j++) {
            executor.submit(() -> {
//                while (i.getAndIncrement() < 2000000000);
                int i = 0;
                while (i++ < 2000000000);
            });
        }

        executor.shutdown();
        while (!executor.isTerminated()) {
            // Ожидание завершения всех потоков
        }
//        LinkedList<Thread> ts = new LinkedList<>();
//        for (int j = 0; j < threadCount; j++) {
//            var thread1 = new Thread() {
//                @Override
//                public void run() {
//                    while (i.getAndIncrement() < 2000000000);
//                }
//            };
//            ts.add(thread1);
//        }
//        for (var thread: ts) {
//            thread.start();
//        }
//        System.out.println("Started");
//        for (var thread: ts) {
//            thread.join();
//        }
    }
}