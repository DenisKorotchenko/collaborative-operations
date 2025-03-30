package ru.dksu.deprecated;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CollaborativeQueueOnThreadPoolHashMap<K, V> {
    static int MIN_SPLIT_SIZE = 64;

    private Buckets<K, V> buckets;
    public static Map<Integer, LinkedList<Long>> rebuildTimes = new HashMap<>();
    private ConcurrentHashMap<RebuildTask, Integer> tasks = new ConcurrentHashMap<>();

    public CollaborativeQueueOnThreadPoolHashMap(int bucketsSize) {
        this.buckets = new Buckets<>(bucketsSize);
        System.out.println("Processors: " + Runtime.getRuntime().availableProcessors());
    }

    private static class RebuildTask<K, V> {
        Buckets<K,V> oldBuckets;
        Buckets<K,V> newBuckets;
        volatile int currentIndex;
        volatile int endIndex;

        public RebuildTask(
                Buckets<K, V> oldBuckets,
                Buckets<K, V> newBuckets,
                int startIndex,
                int endIndex
        ) {
            this.oldBuckets = oldBuckets;
            this.newBuckets = newBuckets;
            this.currentIndex = startIndex;
            this.endIndex = endIndex;
        }

        public void start() {
            while (currentIndex < endIndex) {
                for (var el : oldBuckets.buckets[currentIndex]) {
                    newBuckets.buckets[Math.abs(el.key.hashCode()) % newBuckets.buckets.length].add(el);
                    newBuckets.size.incrementAndGet();
                }
                currentIndex++;
            }
        }

        public RebuildTask<K, V> split() {
            // слишком мало осталось сделать -- нет смысла делить
            if (endIndex - currentIndex < MIN_SPLIT_SIZE) {
                return null;
            }
            int medIndex = (endIndex + currentIndex) / 2;
            var newTask = new RebuildTask<K, V>(
                    oldBuckets,
                    newBuckets,
                    medIndex,
                    endIndex
            );
            endIndex = medIndex;
            // за время разделения полностью прошли левую часть
            if (currentIndex >= endIndex) {
                throw new RuntimeException("Получили не консистентное состояние");
            }

            return newTask;
        }

    }

    static class Buckets<K, V> {
        public ConcurrentLinkedDeque<CollaborativeQueueOnThreadPoolHashMap.Node<K, V>>[] buckets;
        public ReadWriteLock[] rwlocks;
        public AtomicInteger size = new AtomicInteger(0);
//        public Lock overallLock = new ReentrantLock();

        public Buckets(int bucketsSize) {
            buckets = new ConcurrentLinkedDeque[bucketsSize];
            rwlocks = new ReadWriteLock[bucketsSize];
            for (int i = 0; i < bucketsSize; i++) {
                rwlocks[i] = new ReentrantReadWriteLock();
                buckets[i] = new ConcurrentLinkedDeque<>();
            }
        }

        private int bucketIndex(K key) {
            return Math.abs(key.hashCode()) % buckets.length;
        }

        public V get(K key) throws InterruptedException {
            int bucketIndex = bucketIndex(key);
            var lock = rwlocks[bucketIndex].readLock();
            if (!lock.tryLock(10, TimeUnit.MILLISECONDS)) {
                throw new InterruptedException();
            }
            try {
                var bucket = buckets[bucketIndex];
                for (var el : bucket) {
                    if (el.key.equals(key)) {
                        return el.value;
                    }
                }
                return null;
            } finally {
                lock.unlock();
            }
        }

        public V put(K key, V value) throws InterruptedException {
            int bucketIndex = bucketIndex(key);
            var lock = rwlocks[bucketIndex].writeLock();
            if (!lock.tryLock(10, TimeUnit.MILLISECONDS)) {
                throw new InterruptedException();
            }
            try {
                var bucket = buckets[bucketIndex];
                for (var el : bucket) {
                    if (el.key.equals(key)) {
                        var oldValue = el.value;
                        el.value = value;
                        return oldValue;
                    }
                }
                bucket.add(new Node<K, V>(key.hashCode(), key, value, null));
                size.incrementAndGet();
                return null;
            } finally {
                lock.unlock();
            }
        }
    }

    static class Buckets2<K, V> {
        public ConcurrentLinkedDeque<CollaborativeQueueOnThreadPoolHashMap.Node<K, V>>[] buckets;

        private final int nThreads = 5;
        private final ExecutorService threadPool = Executors.newFixedThreadPool(nThreads);


        public void rebuild(int newBucketsNumber) {
            System.gc();
            long nanoStart = System.nanoTime();
            ConcurrentLinkedDeque<CollaborativeQueueOnThreadPoolHashMap.Node<K, V>>[] newBuckets = new ConcurrentLinkedDeque[newBucketsNumber];
            for (int i = 0; i < newBucketsNumber; i++) {
                newBuckets[i] = new ConcurrentLinkedDeque<>();
            }

            List<Runnable> tasks = new LinkedList<>();
            for (int i = 0; i < nThreads; i++) {
                int finalI = i;
                tasks.add(new Runnable() {
                    @Override
                    public void run() {
                        int up = buckets.length / nThreads * (finalI +1);
                        if (finalI == nThreads - 1) {
                            up = buckets.length;
                        }
                        for (int j = buckets.length / nThreads * finalI; j < up; j++) {
                            for (var el: buckets[j]) {
                                newBuckets[Math.abs(el.key.hashCode()) % newBuckets.length].add(el);
                            }
                        }
                    }
                });
            }
            System.out.println("Time rebuild created:   " + (System.nanoTime() - nanoStart));
            nanoStart = System.nanoTime();
            tasks.parallelStream().map((threadPool::submit)).forEach((future -> {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {}
            }));

            buckets = newBuckets;
            Long timeExecuted = System.nanoTime() - nanoStart;
            rebuildTimes.putIfAbsent(buckets.length, new LinkedList<>());
//            rebuildTimes.compute(buckets.length, k -> )
            rebuildTimes.get(buckets.length).add(timeExecuted);
            System.out.println("Time rebuild:           " + (timeExecuted));
        }
    }

    private void helpIfNeed() {
        while (true) {
            try {
                var task = tasks.keys().nextElement();
                var newTask = task.split();
                if (newTask == null) {
                    return;
                }
                tasks.put(newTask, 1);
                newTask.start();
                tasks.remove(newTask);
                return;
            } catch (NoSuchElementException e) {
                return;
            }
        }
    }

    static class Node<K,V> implements Map.Entry<K,V> {
        final int hash;
        final K key;
        V value;
        Node<K,V> next;

        Node(int hash, K key, V value, Node<K,V> next) {
            this.hash = hash;
            this.key = key;
            this.value = value;
            this.next = next;
        }

        public final K getKey()        { return key; }
        public final V getValue()      { return value; }
        public final String toString() { return key + "=" + value; }

        public final int hashCode() {
            return Objects.hashCode(key) ^ Objects.hashCode(value);
        }

        public final V setValue(V newValue) {
            V oldValue = value;
            value = newValue;
            return oldValue;
        }

        public final boolean equals(Object o) {
            if (o == this)
                return true;

            return o instanceof Map.Entry<?, ?> e
                    && Objects.equals(key, e.getKey())
                    && Objects.equals(value, e.getValue());
        }
    }

    private Lock rebuildLock = new ReentrantLock();

    private void rebuild() {
        if (rebuildLock.tryLock()) {
            try {
                var nanoStart = System.nanoTime();
                for (var lock: buckets.rwlocks) {
                    lock.writeLock().lock();
                }
                var newBuckets = new Buckets<K, V>(buckets.buckets.length * 2);
                RebuildTask<K, V> task = new RebuildTask<>(
                        buckets,
                        newBuckets,
                        0,
                        buckets.buckets.length
                );
                tasks.put(task, 1);
                task.start();
                tasks.remove(task);
                while (tasks.size() > 0) {
                    Thread.sleep(10);
                }
                buckets = newBuckets;
                Long timeExecuted = System.nanoTime() - nanoStart;
                rebuildTimes.putIfAbsent(buckets.buckets.length, new LinkedList<>());
                rebuildTimes.get(buckets.buckets.length).add(timeExecuted);
            } catch (InterruptedException ignored) {
            } finally {
                rebuildLock.unlock();
            }
        }
    }

    private void rebuildIfNeed() {
        if (size() > 0.75 * buckets.buckets.length) {
            System.out.println("Current size before rebuild: " + size());
            rebuild();
            System.out.println("Current size after  rebuild: " + size());
        } else if (false) {//size < buckets.buckets.length) {
//            buckets.rebuild(buckets.buckets.length / 2);
//            System.out.println("Current size: " + size());
        }
    }

    public int size() {
        return buckets.size.get();
    }

    public int size2() {
        int ans = 0;
        for (var bucket : buckets.buckets) {
            ans += bucket.size();
        }
        return ans;
    }

//    private ConcurrentLinkedDeque<Node<K, V>> getBucket(Object key) {
//        return buckets.buckets[Math.abs(key.hashCode()) % buckets.buckets.length];
//    }
//
//    private Node<K, V> getNode(Object key) {
//        var node = getBucket(key);
//        for (var el: node) {
//            if (el.key.equals(key)) {
//                return el;
//            }
//        }
//        return null;
//    }

    public V get(K key) {
        while (true) {
            try {
                return buckets.get(key);
            } catch (InterruptedException ignored) {
            }
        }
    }

    public V put(K key, V value) {
        V result = null;
        while (true) {
            try {
                result = buckets.put(key, value);
                break;
            } catch (InterruptedException e) {
                helpIfNeed();
            }
        }
        rebuildIfNeed();
        return result;
    }

    public V remove(K key) {
        throw new RuntimeException("Not implemented yet");
    }
}
