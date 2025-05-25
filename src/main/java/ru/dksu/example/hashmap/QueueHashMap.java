package ru.dksu.example.hashmap;

import contention.abstractions.CompositionalMap;
import ru.dksu.CollaborativeTask;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public class QueueHashMap<K, V> implements CompositionalMap<K, V> {
    // For benchmarks
    private boolean benchmarking = false;
    public Map<Integer, LinkedList<Long>> rebuildTimes = new HashMap<>();
    public Map<Integer, LinkedList<Long>> snapshotTimes = new HashMap<>();

    private Buckets<K, V> buckets;
    private final ReadWriteLock toAllLock = new ReentrantReadWriteLock();

    public QueueHashMap() {
        this(
                16,
                new HashMap<>(),
                new HashMap<>()
        );
    }

    public QueueHashMap(int bucketsSize,
                        Map<Integer, LinkedList<Long>> rebuildTimes,
                        Map<Integer, LinkedList<Long>> snapshotTimes) {
        this(
                bucketsSize,
                rebuildTimes,
                snapshotTimes,
                false
        );
    }

    public QueueHashMap(int bucketsSize,
                        Map<Integer, LinkedList<Long>> rebuildTimes,
                        Map<Integer, LinkedList<Long>> snapshotTimes,
                        boolean benchmarking) {
        this.rebuildTimes = rebuildTimes;
        this.snapshotTimes = snapshotTimes;
        this.buckets = new Buckets<>(bucketsSize);
        for (int i = 0; i < bucketsSize; i++) {
            buckets.buckets[i] = new ArrayDeque<>();
        }
        this.benchmarking = benchmarking;
    }

    static class Node<K,V> implements Entry<K,V> {
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

    static class Buckets<K, V> {
        private static final int TRIES_THRESHOLD = 10;
        private final Deque<Node<K, V>>[] buckets;
        private final int[] seqLocks;
        private final AtomicInteger size = new AtomicInteger(0);

        public Buckets(int bucketsSize) {
            buckets = new ArrayDeque[bucketsSize];
            seqLocks = new int[bucketsSize];
        }

        private int bucketIndex(K key, int size) {
            return Math.abs(key.hashCode()) % size;
        }

        public V get(K key) throws InterruptedException {
            var buckets = this.buckets;
            var varHandle = MethodHandles.arrayElementVarHandle(int[].class);
            int bucketIndex = bucketIndex(key, buckets.length);
            int tries = 0;
            while (true) {
                int before = (int) varHandle.getAcquire(this.seqLocks, bucketIndex);
                if (before % 2 != 0) {
                    tries++;
                    if (tries > TRIES_THRESHOLD) {
                        throw new InterruptedException();
                    }
                    Thread.yield();
                    continue;
                }
                var bucket = buckets[bucketIndex];
                V ans = null;
                if (bucket != null) {
                    try {
                        for (var el : bucket) {
                            if (el.key.equals(key)) {
                                ans = el.value;
                                break;
                            }
                        }
                    } catch (ConcurrentModificationException ignored) {
                        continue;
                    }
                }
                if ((int) varHandle.getAcquire(this.seqLocks, bucketIndex) == before) {
                    return ans;
                }
                tries++;
            }
        }

        public V put(K key, V value, boolean ifAbsent) throws InterruptedException {
            return put(key, value, ifAbsent, true);
        }

        private V put(K key, V value, boolean ifAbsent, boolean changeSize) throws InterruptedException {
            var varHandle = MethodHandles.arrayElementVarHandle(int[].class);
            int tries = 0;
            int before;
            int bucketIndex = -1;
            while (true) {
                bucketIndex = bucketIndex(key, buckets.length);
                before = (int) varHandle.getAcquire(this.seqLocks, bucketIndex);
                if (before % 2 != 0 || !varHandle.compareAndSet(this.seqLocks, bucketIndex, before, before+1)) {
                    tries++;
                    if (tries > TRIES_THRESHOLD) {
                        throw new InterruptedException();
                    }
                    Thread.yield();
                } else {
                    if (bucketIndex(key, buckets.length) != bucketIndex) {
                        continue;
                    }
                    break;
                }
            }

            var bucket = buckets[bucketIndex];
            if (bucket == null) {
                bucket = (buckets[bucketIndex] = new ArrayDeque<>());
            }
            V prev = null;
            for (var el : bucket) {
                if (el.key.equals(key)) {
                    prev = el.value;
                    if (!ifAbsent) {
                        el.value = value;
                    }
                    break;
                }
            }
            if (prev == null) {
                bucket.add(new Node<K, V>(key.hashCode(), key, value, null));
                if (changeSize) {
                    size.incrementAndGet();
                }
            }

            if (!varHandle.compareAndSet(this.seqLocks, bucketIndex, before+1, before+2)) {
                throw new RuntimeException("Hmmm");
            }

            return prev;
        }

        public V remove(K key) throws InterruptedException {
            return remove(key, true);
        }

        private V remove(K key, boolean changeSize) throws InterruptedException {
            var varHandle = MethodHandles.arrayElementVarHandle(int[].class);
            int bucketIndex = -1;
            int tries = 0;
            int before;
            while (true) {
                bucketIndex = bucketIndex(key, buckets.length);
                before = (int) varHandle.getAcquire(this.seqLocks, bucketIndex);
                if (before % 2 != 0 || !varHandle.compareAndSet(this.seqLocks, bucketIndex, before, before+1)) {
                    tries++;
                    if (tries > TRIES_THRESHOLD) {
                        throw new InterruptedException();
                    }
                    Thread.yield();
                } else {
                    if (bucketIndex(key, buckets.length) != bucketIndex) {
                        continue;
                    }
                    break;
                }
            }

            var bucket = buckets[bucketIndex];
            if (bucket == null) {
                bucket = (buckets[bucketIndex] = new ArrayDeque<>());
            }
            V prev = null;
            for (var el : bucket) {
                if (el.key.equals(key)) {
                    prev = el.value;
                    bucket.remove(el);
                    if (changeSize) {
                        size.decrementAndGet();
                    }
                    break;
                }
            }

            if (!varHandle.compareAndSet(this.seqLocks, bucketIndex, before+1, before+2)) {
                throw new RuntimeException("Hmmm");
            }

            return prev;
        }

    }

    public static class KeyValue<K, V> {
        public K key;
        public V value;

        KeyValue(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    public ArrayList<KeyValue<K, V>> snapshot() {
        return snapshotUnsafe();
    }

    public ArrayList<KeyValue<K, V>> snapshotUnsafe() {
        ArrayList<KeyValue<K, V>> snapshot = new ArrayList<>();

        for (var bucket: buckets.buckets) {
            if (bucket != null) {
                for (var el: bucket) {
                    snapshot.add(new KeyValue<>(el.key, el.value));
                }
            }
        }

        return snapshot;
    }

    public V reduce(
            V start,
            BiFunction<V, V, V> function
    ) {
        V res = start;
        for (var bucket: buckets.buckets) {
            if (bucket != null) {
                for (var el: bucket) {
                    res = function.apply(res, el.value);
                }
            }
        }
        return res;
    }

    // Collaborative tasks for rebuilding
    private static class RebuildTask<K, V> implements CollaborativeTask {
        Buckets<K,V> oldBuckets;
        Buckets<K,V> newBuckets;
        int currentIndex;
        int endIndex;

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
            int sizeIncrement = 0;
            while (currentIndex < endIndex) {
                if (oldBuckets.buckets[currentIndex] == null) {
                    currentIndex++;
                    continue;
                }
                for (var el : oldBuckets.buckets[currentIndex]) {
                    while (true) {
                        try {
                            newBuckets.put(el.key, el.value, false, false);
                            break;
                        } catch (InterruptedException ignored) {
                        }
                    }
                    sizeIncrement++;
                }
                currentIndex++;
            }
            newBuckets.size.getAndAdd(sizeIncrement);
        }
    }

    private void rebuildUnsafe(int newSize) {
        var nanoStart = System.nanoTime();

        // создаем новые бакеты
        var newBuckets = new Buckets<K, V>(newSize);

        new RebuildTask<>(
                buckets,
                newBuckets,
                0,
                buckets.buckets.length
        ).start();

        buckets = newBuckets;

        if (benchmarking) {
            Long timeExecuted = System.nanoTime() - nanoStart;
            rebuildTimes.putIfAbsent(buckets.buckets.length, new LinkedList<>());
            rebuildTimes.get(buckets.buckets.length).add(timeExecuted);
        }
    }

    private void rebuildIfNeed() {
        while (true) {
            if (int_size() > 0.75 * buckets.buckets.length) {
                if (toAllLock.writeLock().tryLock()) {
                    try {
                        rebuildUnsafe(buckets.buckets.length * 2);
                    } finally {
                        toAllLock.writeLock().unlock();
                    }
                } else {
                    Thread.yield();
                }
            } else if (int_size() < 0.25 * buckets.buckets.length && buckets.buckets.length > 2) {//size < buckets.buckets.length) {
                if (toAllLock.writeLock().tryLock()) {
                    try {
                        rebuildUnsafe(buckets.buckets.length / 2);
                    } finally {
                        toAllLock.writeLock().unlock();
                    }
                } else {
                    Thread.yield();
                }
            } else {
                break;
            }
        }
    }




    @Override
    public V putIfAbsent(K k, V v) {
        return put(k, v, true);
    }

    @Override
    public boolean remove(Object key, Object value) {
        return CompositionalMap.super.remove(key, value);
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        return CompositionalMap.super.replace(key, oldValue, newValue);
    }

    @Override
    public V replace(K key, V value) {
        return CompositionalMap.super.replace(key, value);
    }

    @Override
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        return CompositionalMap.super.computeIfAbsent(key, mappingFunction);
    }

    @Override
    public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
        return CompositionalMap.super.computeIfPresent(key, remappingFunction);
    }

    @Override
    public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        return CompositionalMap.super.merge(key, value, remappingFunction);
    }

    @Override
    public void clear() {
        this.buckets = new Buckets<>(16);
    }

    @Override
    public Set<K> keySet() {
        return Set.of();
    }

    @Override
    public Collection<V> values() {
        return List.of();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return Set.of();
    }

    @Override
    public V getOrDefault(Object key, V defaultValue) {
        return CompositionalMap.super.getOrDefault(key, defaultValue);
    }

    @Override
    public void forEach(BiConsumer<? super K, ? super V> action) {
        CompositionalMap.super.forEach(action);
    }

    @Override
    public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
        CompositionalMap.super.replaceAll(function);
    }

    private int int_size() {
        return buckets.size.get();
    }

    @Override
    public int size() {
        // TODO: incorrect size operation for benchmarking
        return this.snapshot().size();
//        return int_size();
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean containsKey(Object o) {
        return false;
    }

    @Override
    public boolean containsValue(Object o) {
        return false;
    }

    public int sizeUnsafe() {
        int ans = 0;
        for (var bucket : buckets.buckets) {
            if (bucket != null) {
                ans += bucket.size();
            }
        }
        return ans;
    }

    @Override
    public V get(Object key) {
        while (true) {
            try {
                return buckets.get((K) key);
            } catch (InterruptedException ignored) {
            }
        }
    }

    public V put(K key, V value) {
        return put(key, value, false);
    }

    public V put(K key, V value, boolean ifAbsent) {
        V result;
        while (true) {
            if (toAllLock.readLock().tryLock()) {
                try {
                    result = buckets.put(key, value, ifAbsent);
                    break;
                } catch (InterruptedException ignored) {
                } finally {
                    toAllLock.readLock().unlock();
                }
            } else {
                Thread.yield();
            }
        }
        rebuildIfNeed();
        return result;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public V remove(Object key) {
        V result;
        while (true) {
            if (toAllLock.readLock().tryLock()) {
                try {
                    result = buckets.remove((K) key);
                    break;
                } catch (InterruptedException ignored) {
                } finally {
                    toAllLock.readLock().unlock();
                }
            } else {
                Thread.yield();
            }
        }
        rebuildIfNeed();
        return result;
    }
}
