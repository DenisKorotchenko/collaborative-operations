package ru.dksu;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class CollaborativeHashMap<K, V>{
    private final ExecutorService threadPool;
    LinkedList<Node<K, V>>[] buckets;

    public CollaborativeHashMap() {
        int bucketsSize = 16;
        buckets = new LinkedList[bucketsSize];
        for (int i = 0; i < bucketsSize; i++) {
            buckets[i] = new LinkedList<>();
        }
        threadPool = Executors.newFixedThreadPool(8);
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
        if (size() > 4 * buckets.length) {
            rebuild(buckets.length * 2);
        } else if (size() < buckets.length) {
            rebuild(buckets.length / 2);
        }
    }

    private void rebuild(int newBucketsNumber) {
        long nanoStart = System.nanoTime();
        LinkedList<Node<K, V>>[] newBuckets = new LinkedList[newBucketsNumber];
        for (int i = 0; i < newBucketsNumber; i++) {
            newBuckets[i] = new LinkedList<>();
        }
        var tasks = Arrays.stream(buckets).map((bucket) -> {
            return (Runnable) () -> {
                for (Node<K, V> el: bucket) {
                    newBuckets[el.key.hashCode() % newBuckets.length].add(el);
                }
            };
        });
        var features = tasks.map((task) -> threadPool.submit(task));
        features.forEach((feature) -> {
            try {
                feature.get();
            } catch (Exception e) {}
        });
        buckets = newBuckets;
        System.out.println("Time rebuild: " + (System.nanoTime() - nanoStart));
    }

    public int size() {
        int sz = 0;
        for (var node: buckets) {
            sz += node.size();
        }
        return sz;
    }

    private LinkedList<Node<K, V>> getBucket(Object key) {
        return buckets[key.hashCode() % buckets.length];
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
        rebuildIfNeed();
        return null;
    }

    public V remove(K key) {
        var node = getNode(key);
        if (node == null) return null;
        var bucket = getBucket(key);
        bucket.remove(node);
        rebuildIfNeed();
        return null;
    }
}
