package ru.dksu;

import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

public class InitializationForkJoinPool<K, V> {
    private ForkJoinPool forkJoinPool;
    public InitializationForkJoinPool() {
        System.out.println("Processors: " + Runtime.getRuntime().availableProcessors());
        forkJoinPool = new ForkJoinPool(2);
    }

    static int[] array = new int[10000000];
    static Random random = new Random(239);

    void start() {
        var rebuild = new Rebuild(0, array.length, 1);
        System.gc();
        long nanoStart = System.nanoTime();
        forkJoinPool.invoke(rebuild);
        System.out.println("Time:      " + (System.nanoTime() - nanoStart));
    }

    static class Rebuild<K, V> extends RecursiveTask<Void> {
        private final int from, to, depth;

        Rebuild(
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
                var left = new Rebuild(from, med, depth+1);
                var right = new Rebuild(med, to, depth+1);
                left.fork();
                right.fork();
                left.join();
                right.join();
                return null;
            }
            for (int i = from; i < to; i++) {
                array[i] = random.nextInt();
            }
            return null;
        }
    }


}
