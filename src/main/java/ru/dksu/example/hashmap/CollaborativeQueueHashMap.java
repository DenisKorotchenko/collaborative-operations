package ru.dksu.example.hashmap;

import contention.abstractions.CompositionalMap;
import ru.dksu.CollaborativeQueue;
import ru.dksu.CollaborativeTask;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CollaborativeQueueHashMap<K, V> implements CompositionalMap<K, V> {
    // For benchmarks
    private boolean benchmarking = false;
    public Map<Integer, LinkedList<Long>> rebuildTimes = new HashMap<>();
    public Map<Integer, LinkedList<Long>> snapshotTimes = new HashMap<>();

    private Buckets<K, V> buckets;
    private final CollaborativeQueue<CollaborativeTask> collaborativeQueue = new CollaborativeQueue<>();
    private final ReadWriteLock collaborativeLock = new ReentrantReadWriteLock();

    // How much buckets put into one collaborative task
    private final int delta = 1024;

    public CollaborativeQueueHashMap() {
        this(
                16,
                new HashMap<>(),
                new HashMap<>()
        );
    }

    public CollaborativeQueueHashMap(int bucketsSize,
                                     Map<Integer, LinkedList<Long>> rebuildTimes,
                                     Map<Integer, LinkedList<Long>> snapshotTimes) {
        this(
                bucketsSize,
                rebuildTimes,
                snapshotTimes,
                false
        );
    }

    public CollaborativeQueueHashMap(int bucketsSize,
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

    public static class KeyValue<K, V> implements Entry<K, V> {
        public K key;
        public V value;

        KeyValue(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            V oldValue = this.value;
            this.value = value;
            return oldValue;
        }
    }

    // Tasks for snapshot
    private static class CopyToPartial<K, V> implements CollaborativeTask {
        Collection<KeyValue<K, V>> partialSnapshot;
        Buckets<K, V> buckets;
        int currentIndex;
        int endIndex;

        public CopyToPartial(
                Collection<KeyValue<K, V>> partialSnapshot,
                Buckets<K, V> buckets,
                int startIndex,
                int endIndex
        ) {
            this.partialSnapshot = partialSnapshot;
            this.buckets = buckets;
            this.currentIndex = startIndex;
            this.endIndex = endIndex;
        }

        @Override
        public void start() {
            for (; currentIndex < endIndex; currentIndex++) {
                if (buckets.buckets[currentIndex] == null) continue;
                for (var el: buckets.buckets[currentIndex]) {
                    partialSnapshot.add(new KeyValue<>(el.getKey(), el.getValue()));
                }
            }
        }
    }

    private static class CopyPartialToSnapshot<K, V> implements CollaborativeTask {
        List<Collection<KeyValue<K, V>>> partialSnapshots;
        ArrayList<KeyValue<K, V>> snapshot;
        int snapshotIndex;
        int currentIndex;
        int endIndex;

        public CopyPartialToSnapshot(
                List<Collection<KeyValue<K, V>>> partialSnapshots,
                int beginSnapshotIndex,
                ArrayList<KeyValue<K, V>> snapshot,
                int startIndex,
                int endIndex
        ) {
            this.partialSnapshots = partialSnapshots;
            this.snapshotIndex = beginSnapshotIndex;
            this.snapshot = snapshot;
            this.currentIndex = startIndex;
            this.endIndex = endIndex;
        }

        @Override
        public void start() {
            for (; currentIndex < endIndex; currentIndex++) {
                for (var el: partialSnapshots.get(currentIndex)) {
                    snapshot.set(snapshotIndex++, el);
                }
            }
        }
    }

    public ArrayList<KeyValue<K, V>> snapshot() {
        List<Collection<KeyValue<K, V>>> partialSnapshots = new ArrayList<>();
        ArrayList<KeyValue<K, V>> snapshot = new ArrayList<>();
        final int[] overallSize = {0};
        long startTime = System.nanoTime();

        collaborativeQueue.multiStageCollaborativeOperation(
                collaborativeLock.writeLock(),
                List.of(
                        (queue) -> {
                            int startIndex = 0;
                            int endIndex = delta;
                            while (startIndex < buckets.buckets.length) {
                                var partialSnapshot = new ArrayList<KeyValue<K, V>>();
                                partialSnapshots.add(partialSnapshot);
                                CollaborativeTask task = new CopyToPartial<>(
                                        partialSnapshot,
                                        buckets,
                                        startIndex,
                                        Math.min(buckets.buckets.length, endIndex)
                                );
                                startIndex += delta;
                                endIndex += delta;
                                collaborativeQueue.add(task);
                            }
                        },
                        (queue) -> {
                            overallSize[0] = internalSize();
                            int curSize = 0;
                            int ind = 0;
                            for (var part: partialSnapshots) {
                                CollaborativeTask task = new CopyPartialToSnapshot<>(
                                        partialSnapshots,
                                        curSize,
                                        snapshot,
                                        ind,
                                        ++ind
                                );
                                curSize += part.size();
                                snapshot.addAll(part);
                                collaborativeQueue.add(task);
                            }
                        }
                )
        );

        if (benchmarking) {
            long timeExecuted = System.nanoTime() - startTime;
            snapshotTimes.putIfAbsent(overallSize[0], new LinkedList<>());
            snapshotTimes.get(overallSize[0]).add(timeExecuted);
        }

        return snapshot;
    }

    public ArrayList<KeyValue<K, V>> snapshotUnsafe() {
        List<Collection<KeyValue<K, V>>> partialSnapshots = new ArrayList<>();

        long startTime = System.nanoTime();

        int delta = this.delta;
        int startIndex = 0;
        int endIndex = delta;
        while (endIndex < buckets.buckets.length) {
            var partialSnapshot = new ArrayList<KeyValue<K, V>>();
            partialSnapshots.add(partialSnapshot);
            CollaborativeTask task = new CopyToPartial<>(
                    partialSnapshot,
                    buckets,
                    startIndex,
                    endIndex
            );
            startIndex += delta;
            endIndex += delta;
            collaborativeQueue.add(task);
        }
        var partialSnapshot = new ArrayList<KeyValue<K, V>>();
        partialSnapshots.add(partialSnapshot);
        CollaborativeTask taskFinal = new CopyToPartial<>(
                partialSnapshot,
                buckets,
                startIndex,
                buckets.buckets.length
        );
        collaborativeQueue.add(taskFinal);

        int overallSize = internalSize();
        ArrayList<KeyValue<K, V>> snapshot = new ArrayList<>();
        collaborativeQueue.helpIfNeeded();
        collaborativeQueue.waitForFinish();

        int curSize = 0;
        int ind = 0;
        for (var part: partialSnapshots) {
            CollaborativeTask task = new CopyPartialToSnapshot<>(
                    partialSnapshots,
                    curSize,
                    snapshot,
                    ind,
                    ++ind
            );
            curSize += part.size();
            snapshot.addAll(part);
            collaborativeQueue.add(task);
        }

        collaborativeQueue.helpIfNeeded();
        collaborativeQueue.waitForFinish();

        if (benchmarking) {
            long timeExecuted = System.nanoTime() - startTime;
            snapshotTimes.putIfAbsent(overallSize, new LinkedList<>());
            snapshotTimes.get(overallSize).add(timeExecuted);
        }

        return snapshot;
    }

    public V reduce(
            BiFunction<V, V, V> function
    ) {
        return collaborativeQueue.collaborativeOperation(
                collaborativeLock.writeLock(),
                (collaborativeQueue ->
                        reducePartialUnsafe(collaborativeQueue, function)),
                (results -> {
                    V result = null;
                    for (V partResult : results) {
                        if (result == null) {
                            result = partResult;
                        } else {
                            if (partResult != null) {
                                result = function.apply(result, partResult);
                            }
                        }
                    }
                    return result;
                })
        );
    }

    // Collaborative tasks for reduce operation
    private class ReducePartTask implements CollaborativeTask {
        final Buckets<K, V> buckets;
        int currentIndex;
        final int endIndex;
        final BiFunction<V, V, V> function;
        final Consumer<V> saveResult;

        ReducePartTask(
                Buckets<K, V> buckets,
                int startIndex,
                int endIndex,
                BiFunction<V, V, V> function,
                Consumer<V> saveResult
        ) {
            this.buckets = buckets;
            this.currentIndex = startIndex;
            this.endIndex = endIndex;
            this.function = function;
            this.saveResult = saveResult;
        }

        @Override
        public void start() {
            V res = null;
            for (; currentIndex < endIndex; currentIndex++) {
                if (buckets.buckets[currentIndex] != null) {
                    for (var el : buckets.buckets[currentIndex]) {
                        if (res != null) {
                            if (el.value != null) {
                                res = function.apply(res, el.value);
                            }
                        } else {
                            res = el.value;
                        }
                    }
                }
            }
            saveResult.accept(res);
        }
    }



    private List<V> reducePartialUnsafe(
            CollaborativeQueue<CollaborativeTask> collaborativeQueue,
            BiFunction<V, V, V> function
    ) {
        List<V> results = Collections.synchronizedList(new ArrayList<>());
        int startIndex = 0;
        int endIndex = delta;
        while (startIndex < buckets.buckets.length) {
            ReducePartTask reducePartTask = new ReducePartTask(
                    buckets, startIndex, Math.min(endIndex, buckets.buckets.length),
                    function, results::add);
            startIndex += delta;
            endIndex += delta;
            collaborativeQueue.add(reducePartTask);
        };
        return results;
    }

    // Collaborative tasks for rebuilding
    private static class RebuildTask<K, V> implements CollaborativeTask  {
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

        // create new buckets
        var newBuckets = new Buckets<K, V>(newSize);

        int delta = this.delta;
        int startIndex = 0;
        int endIndex = delta;
        while (endIndex < buckets.buckets.length) {
            RebuildTask<K, V> task = new RebuildTask<>(
                    buckets,
                    newBuckets,
                    startIndex,
                    endIndex
            );
            startIndex += delta;
            endIndex += delta;
            collaborativeQueue.add(task);
        }
        RebuildTask<K, V> taskFinal = new RebuildTask<>(
                buckets,
                newBuckets,
                startIndex,
                buckets.buckets.length
        );
        collaborativeQueue.add(taskFinal);
        collaborativeQueue.helpIfNeeded();
        collaborativeQueue.waitForFinish();

        buckets = newBuckets;

        if (benchmarking) {
            Long timeExecuted = System.nanoTime() - nanoStart;
            rebuildTimes.putIfAbsent(buckets.buckets.length, new LinkedList<>());
            rebuildTimes.get(buckets.buckets.length).add(timeExecuted);
        }
    }

    private void rebuildIfNeed() {
        while (true) {
            if (internalSize() > 0.75 * buckets.buckets.length) {
                if (collaborativeLock.writeLock().tryLock()) {
                    try {
                        rebuildUnsafe(buckets.buckets.length * 2);
                    } finally {
                        collaborativeLock.writeLock().unlock();
                    }
                } else {
                    collaborativeQueue.helpIfNeeded();
                    Thread.yield();
                }
            } else if (internalSize() < 0.25 * buckets.buckets.length && buckets.buckets.length > 2) {//size < buckets.buckets.length) {
                if (collaborativeLock.writeLock().tryLock()) {
                    try {
                        rebuildUnsafe(buckets.buckets.length / 2);
                    } finally {
                        collaborativeLock.writeLock().unlock();
                    }
                } else {
                    collaborativeQueue.helpIfNeeded();
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
        return snapshot().stream().map((kv) -> kv.key).collect(Collectors.toSet());
    }

    @Override
    public Collection<V> values() {
        return snapshot().stream().map((kv) -> kv.value).toList();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return Set.copyOf(snapshot());
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

    private int internalSize() {
        return buckets.size.get();
    }

    @Override
    public int size() {
        return internalSize();
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
            if (collaborativeLock.readLock().tryLock()) {
                try {
                    result = buckets.put(key, value, ifAbsent);
                    break;
                } catch (InterruptedException ignored) {
                } finally {
                    collaborativeLock.readLock().unlock();
                }
            } else {
                collaborativeQueue.helpIfNeeded();
                Thread.yield();
            }
        }
        rebuildIfNeed();
        return result;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        map.forEach(this::put);
    }

    @Override
    public V remove(Object key) {
        V result;
        while (true) {
            if (collaborativeLock.readLock().tryLock()) {
                try {
                    result = buckets.remove((K) key);
                    break;
                } catch (InterruptedException ignored) {
                } finally {
                    collaborativeLock.readLock().unlock();
                }
            } else {
                collaborativeQueue.helpIfNeeded();
                Thread.yield();
            }
        }
        rebuildIfNeed();
        return result;
    }
}
