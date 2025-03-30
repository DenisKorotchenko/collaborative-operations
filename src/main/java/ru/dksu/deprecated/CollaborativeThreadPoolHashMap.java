package ru.dksu.deprecated;

import java.util.*;
import java.util.concurrent.*;

public class CollaborativeThreadPoolHashMap<K, V>{
    private Buckets<K, V> buckets = new Buckets<>();
    private int size = 0;
    public static Map<Integer, LinkedList<Long>> rebuildTimes = new HashMap<>();

    public CollaborativeThreadPoolHashMap() {
        int bucketsSize = 16;
        buckets.buckets = new ConcurrentLinkedDeque[bucketsSize];
        for (int i = 0; i < bucketsSize; i++) {
            buckets.buckets[i] = new ConcurrentLinkedDeque<>();
        }
        System.out.println("Processors: " + Runtime.getRuntime().availableProcessors());
    }

    static class Buckets<K, V> {
        public ConcurrentLinkedDeque<CollaborativeThreadPoolHashMap.Node<K, V>>[] buckets;

        private final int nThreads = 5;
        private final ExecutorService threadPool = Executors.newFixedThreadPool(nThreads);

        public void rebuild(int newBucketsNumber) {
            System.gc();
            long nanoStart = System.nanoTime();
            ConcurrentLinkedDeque<CollaborativeThreadPoolHashMap.Node<K, V>>[] newBuckets = new ConcurrentLinkedDeque[newBucketsNumber];
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

    private void rebuildIfNeed() {
        if (size > 0.75 * buckets.buckets.length) {
            buckets.rebuild(buckets.buckets.length * 2);
            System.out.println("Current size: " + size());
        } else if (false) {//size < buckets.buckets.length) {
            buckets.rebuild(buckets.buckets.length / 2);
            System.out.println("Current size: " + size());
        }
    }

    public int size() {
        return size;
    }

    private ConcurrentLinkedDeque<Node<K, V>> getBucket(Object key) {
        return buckets.buckets[Math.abs(key.hashCode()) % buckets.buckets.length];
    }

    private Node<K, V> getNode(Object key) {
        var node = getBucket(key);
        for (var el: node) {
            if (el.key.equals(key)) {
                return el;
            }
        }
        return null;
    }

    public V get(K key) {
        var node = getNode(key);
        if (node == null) {
            return null;
        }
        return node.value;
    }

    public V put(K key, V value) {
        var node = getNode(key);
        if (node != null) {
            V oldValue = node.value;
            node.value = value;
            return oldValue;
        }

        var bucket = getBucket(key);
        bucket.add(new Node<K, V>(key.hashCode(), key, value, null));
        size++;
        rebuildIfNeed();
        return null;
    }

    public V remove(K key) {
        var node = getNode(key);
        if (node == null) return null;
        var bucket = getBucket(key);
        bucket.remove(node);
        size--;
        rebuildIfNeed();
        return null;
    }
}
