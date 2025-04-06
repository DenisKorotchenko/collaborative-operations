package ru.dksu;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        Random r = new Random();
        r.setSeed(239);
        int elems = 500000;
        int iters = 100;
        int itersDelta = iters / 5;
        int threadsMin = Integer.valueOf(args[0]);
        int threadsMax = Integer.valueOf(args[1]);
        System.out.println("Threads, from: " + threadsMin + "; to: " + threadsMax);
        HashMap<Integer, Map<Integer, LinkedList<Long>>> mSnapshotTimes = new HashMap<>(), mRebuildTimes = new HashMap<>();
        for (int threads = threadsMin; threads <= threadsMax; threads++) {
            mRebuildTimes.put(threads, new HashMap<>());
            mSnapshotTimes.put(threads, new HashMap<>());
        }

        for (int q = 0; q < iters; q++) {
//            var collaborativeHashMap = new CollaborativeQueueHashMap<>(16);
//            var baseHashMap = new HashMap<Integer, Integer>();
            for (int threads = threadsMin; threads <= threadsMax; threads++) {
                var startTime = System.nanoTime();
                AtomicInteger done = new AtomicInteger(0);
                LinkedList<Thread> runs = new LinkedList<>();
                var collaborativeHashMap = new CollaborativeQueueHashMap<>(16, mRebuildTimes.get(threads), mSnapshotTimes.get(threads));
                for (int i = 0; i < threads; i++) {
                    int finalI = i;
                    var thread = new Thread() {
                        @Override
                        public void run() {
                            boolean flg = false;
                            if (finalI == 0) {
                                flg = true;
                            }
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
                                if (finalI == 0 && done.get() > elems / 2000 && flg) {
                                    flg = false;
                                    System.out.println(collaborativeHashMap.snapshot().length);
                                }
                            }
                            System.out.println("Finish thread " + Thread.currentThread().getName() + " Operations count: " + operations);
                        }
                    };
                    runs.add(thread);
                }
                for (var thread : runs) {
                    thread.start();
                }
                for (var thread : runs) {
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
        }

        for (int threads = threadsMin; threads <= threadsMax; threads++) {
            var rebuildTimes = mRebuildTimes.get(threads);
            var snapshotTimes = mSnapshotTimes.get(threads);

            var rebuildKeys = rebuildTimes.keySet().stream().sorted().toArray();
            var rebuildKey = rebuildKeys[rebuildKeys.length - 1];
            var rebuildValues = rebuildTimes.get(rebuildKey).stream().sorted().toList().subList(itersDelta, iters - itersDelta);
            Long rebuildValuesSum = 0L;
            for (Long value : rebuildValues) {
                rebuildValuesSum += value;
            }
            Long rebuildMeanValue = rebuildValuesSum / rebuildValues.size();
            System.out.println("Threads: " + threads + ". Mean rebuild time: " + rebuildMeanValue);

            Long snapshotValuesSum = 0L;
            List<Long> snapshotValues = new LinkedList<Long>();
            for (var el: snapshotTimes.values()) {
                snapshotValues.addAll(el);
            }
            snapshotValues = snapshotValues.stream().sorted().toList().subList(itersDelta, iters - itersDelta);
            for (Long value : snapshotValues) {
                snapshotValuesSum += value;
            }

            var snapshotMeanValue = snapshotValuesSum / snapshotValues.size();
            System.out.println("Threads: " + threads + ". Mean snapshot time: " + snapshotMeanValue);
        }
    }
}
