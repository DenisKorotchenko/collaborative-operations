package ru.dksu;

import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

public class InitializationForkJoinPool<K, V> {
    private ForkJoinPool forkJoinPool;
    public InitializationForkJoinPool() {
        System.out.println("Processors: " + Runtime.getRuntime().availableProcessors());
        forkJoinPool = new ForkJoinPool(1);
    }

    static int[] array = new int[100000000];
    static Random random = new Random(239);

    long start() {
        var rebuild = new Initialize(0, array.length, 1);
        System.gc();
        long nanoStart = System.nanoTime();
        forkJoinPool.invoke(rebuild);
        long nanoFinish = System.nanoTime();
        System.out.println("Time:      " + (nanoFinish - nanoStart));
        return nanoFinish - nanoStart;
    }

    static class Initialize extends RecursiveTask<Void> {
        private final int from, to, depth;

        Initialize(
                int from,
                int to,
                int depth
        ) {
            this.from = from;
            this.to = to;
            this.depth = depth;
        }

        @Override
        protected Void compute() {
            if (depth < 3) {
                int med = (from + to) / 2;
                var left = new Initialize(from, med, depth+1);
                var right = new Initialize(med, to, depth+1);
                left.fork();
                right.fork();
                left.join();
                right.join();
                return null;
            }
//            System.out.println(Thread.currentThread().toString() + " started");
            for (int i = from; i < to; i++) {
                array[i] = 239;
            }
//            System.out.println(Thread.currentThread().toString() + " finished");
            return null;
        }
    }


}
