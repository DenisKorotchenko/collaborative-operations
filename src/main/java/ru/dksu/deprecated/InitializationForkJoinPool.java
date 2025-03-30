package ru.dksu.deprecated;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;


public class InitializationForkJoinPool<K, V> {
    private final ForkJoinPool forkJoinPool;
    public InitializationForkJoinPool() {
        System.out.println("Processors: " + Runtime.getRuntime().availableProcessors());
        forkJoinPool = new ForkJoinPool(4);
    }

    private final int[] array = new int[1000000000];
//    static Random random = new Random(239);

    long start(int i) {
        var rebuild = new Initialize(0, array.length, 1, i);
        System.gc();
        long nanoStart = System.nanoTime();
        forkJoinPool.invoke(rebuild);
        long nanoFinish = System.nanoTime();
        System.out.println("Time:      " + (nanoFinish - nanoStart));
        return nanoFinish - nanoStart;
    }

    class Initialize extends RecursiveTask<Void> {
        private final int from, to, depth;
        private final int x;

        Initialize(
                int from,
                int to,
                int depth,
                int x
        ) {
            this.from = from;
            this.to = to;
            this.depth = depth;
            this.x = x;
        }

        @Override
        protected Void compute() {
            System.out.println("St");
            if (depth < 3) {
                int med = (from + to) / 2;
                var left = new Initialize(from, med, depth+1, x);
//                var right = new Initialize(med, to, depth+1);
                left.fork();
//                right.fork();
                for (int i = med; i < to; i++) {
                    array[i] = x * 239 + 7;
                }
                left.join();
//                right.join();
                return null;
            }
//            System.out.println(Thread.currentThread().toString() + " started");
            for (int i = from; i < to; i++) {
                array[i] = x * 239 + 7;
            }
//            System.out.println(Thread.currentThread().toString() + " finished");
            return null;
        }
    }


}
