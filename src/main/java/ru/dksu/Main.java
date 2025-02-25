package ru.dksu;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class Main {
    public static void main(String[] args) {
        var array = new InitializationForkJoinPool(1);
        array.start(0);
        array.start(1);
        int n = 20;
        long sum = 0;
        for (int i = 0; i < n; i++) {
            sum += array.start(i);
        }
        System.out.println("Mean time: " + sum / n);
    }

    private static final int[] array = new int[1000000000];

    public static class InitializationFixedThreadPool {
        private final ExecutorService threadPoolExecutor;
        private final int nThreads;

        public InitializationFixedThreadPool(int nThreads) {
            threadPoolExecutor = Executors.newFixedThreadPool(nThreads);
            this.nThreads = nThreads;
        }

        long start(int val) {
            System.gc();
            long nanoStart = System.nanoTime();
            List<Runnable> list = new ArrayList<>();
            for (int t = 0; t < nThreads; t++) {
                int finalT = t;
                list.add(new Runnable() {
                    @Override
                    public void run() {
                        System.out.println("Started: " + Thread.currentThread());
                        int from = (int)((long) array.length * (long) finalT / (long) nThreads);
                        int to = (int)((long) array.length * (long) (finalT + 1) / (long) nThreads);
                        long stTime = System.nanoTime();
                        for (long i = from; i < to; i++) {
                            array[(int)i] = val * 239 + 7;
                        }
                        System.out.println("Finished: " + Thread.currentThread() + ", time: " + (System.nanoTime() - stTime));
                    }
                });
            }
            list.parallelStream().map((threadPoolExecutor::submit)).forEach((future -> {
                try {
                    future.get();
                } catch (InterruptedException e) {

                } catch (ExecutionException e) {

                }
            }));

            long nanoFinish = System.nanoTime();
            System.out.println("Time:      " + (nanoFinish - nanoStart));
            return nanoFinish - nanoStart;
        }
    }

    public static class InitializationForkJoinPool {
        private final ForkJoinPool forkJoinPool;

        public InitializationForkJoinPool(int parallelism) {
            System.out.println("Processors: " + Runtime.getRuntime().availableProcessors());
            forkJoinPool = new ForkJoinPool(parallelism);
            System.out.println("Parallelism: " + forkJoinPool.getParallelism());
        }
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
                if (depth < 3) {
                    int med = (from + to) / 2;
                    var left = new Initialize(from, med, depth + 1, x);
                    var right = new Initialize(med, to, depth + 1, x);
                    left.fork();
                    right.fork();
                    left.join();
                    right.join();
                    return null;
                }
                System.out.println(Thread.currentThread().toString() + " started");
                long stTime = System.nanoTime();
                System.out.println(to - from);
                for (int i = from; i < to; i++) {
                    array[i] = x * 239 + 7;
                }
                System.out.println(Thread.currentThread().toString() + " finished" + ", time: " + (System.nanoTime() - stTime));
                return null;
            }
        }


    }
}
