package ru.dksu;

import java.util.*;
import java.util.concurrent.*;

public class CollaborativeHashMap<K, V>{
    private Buckets<K, V> buckets = new Buckets<>();
    private int size = 0;

    public CollaborativeHashMap() {
        int bucketsSize = 16;
        buckets.buckets = new ConcurrentLinkedDeque[bucketsSize];
        for (int i = 0; i < bucketsSize; i++) {
            buckets.buckets[i] = new ConcurrentLinkedDeque<>();
        }
        System.out.println("Processors: " + Runtime.getRuntime().availableProcessors());
    }

    static class BucketsRebuild<K, V> extends RecursiveTask<Void> {
        private final ConcurrentLinkedDeque<CollaborativeHashMap.Node<K, V>>[] buckets;
        private final ConcurrentLinkedDeque<CollaborativeHashMap.Node<K, V>>[] newBuckets;
        private final int from, to;
        BucketsRebuild(ConcurrentLinkedDeque<CollaborativeHashMap.Node<K, V>>[] buckets,
                       ConcurrentLinkedDeque<CollaborativeHashMap.Node<K, V>>[] newBuckets,
                       int from,
                       int to) {
            this.buckets = buckets;
            this.newBuckets = newBuckets;
            this.from = from;
            this.to = to;
        }

        @Override
        protected Void compute() {
            if (to - from > 1) {
                int med = (from + to) / 2;
                var left = new BucketsRebuild(buckets, newBuckets, from, med);
                var right = new BucketsRebuild(buckets,  newBuckets, med, to);
                left.fork();
                right.fork();
                left.join();
                right.join();
                return null;
            }
            for (int i = from; i < to; i++) {
                for (var el: buckets[i]) {
                    newBuckets[Math.abs(el.key.hashCode()) % newBuckets.length].add(el);
                }
            }
            return null;
        }
    }

    static class Buckets<K, V> {
        public ConcurrentLinkedDeque<CollaborativeHashMap.Node<K, V>>[] buckets;

        // FORK JOIN POOL ========================================================
        private final ForkJoinPool forkJoinPool = new ForkJoinPool(5);

        public void rebuild(int newBucketsNumber) {
            System.gc();
            long nanoStart = System.nanoTime();
            ConcurrentLinkedDeque<CollaborativeHashMap.Node<K, V>>[] newBuckets = new ConcurrentLinkedDeque[newBucketsNumber];
            for (int i = 0; i < newBucketsNumber; i++) {
                newBuckets[i] = new ConcurrentLinkedDeque<>();
            }
            var rebuild = new BucketsRebuild(buckets, newBuckets, 0, buckets.length);
            System.out.println("Time rebuild created:   " + (System.nanoTime() - nanoStart));
            nanoStart = System.nanoTime();
            forkJoinPool.invoke(rebuild);
            buckets = newBuckets;
            System.out.println("Time rebuild:           " + (System.nanoTime() - nanoStart));
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
