package ru.dksu;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CollaborativeQueueHashMap<K, V> {
    private Buckets<K, V> buckets;
    public static Map<Integer, LinkedList<Long>> rebuildTimes = new HashMap<>();
    public static Map<Integer, LinkedList<Long>> snapshotTimes = new HashMap<>();
    private final CollaborativeQueue<CollaborativeTask> collaborativeQueue = new CollaborativeQueue<>();

    public CollaborativeQueueHashMap(int bucketsSize) {
        this.buckets = new Buckets<>(bucketsSize);
        for (int i = 0; i < bucketsSize; i++) {
            buckets.buckets[i] = new ArrayDeque<>();
        }
        for (int i = 0; i < buckets.rwlocks.length; i++) {
            buckets.rwlocks[i] = new ReentrantReadWriteLock();
        }
        System.out.println("Processors: " + Runtime.getRuntime().availableProcessors());
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

    static class Buckets<K, V> {
        private static final int TRIES_THRESHOLD = 10;
        final private Random random = new Random();
        public Deque<CollaborativeQueueHashMap.Node<K, V>>[] buckets;
        public ReadWriteLock[] rwlocks;
        public int[] seqLocks;
        public AtomicInteger size = new AtomicInteger(0);

        public Buckets(int bucketsSize) {
            buckets = new ArrayDeque[bucketsSize];
            seqLocks = new int[bucketsSize];
            rwlocks = new ReadWriteLock[bucketsSize];
        }

        private int bucketIndex(K key) {
            return Math.abs(key.hashCode()) % buckets.length;
        }

        public V get(K key) throws InterruptedException {
            var varHandle = MethodHandles.arrayElementVarHandle(int[].class);
            int bucketIndex = bucketIndex(key);
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
                    for (var el : bucket) {
                        if (el.key.equals(key)) {
                            ans = el.value;
                            break;
                        }
                    }
                }
                if ((int) varHandle.getAcquire(this.seqLocks, bucketIndex) == before) {
                    return ans;
                }
            }
        }

        public V put(K key, V value) throws InterruptedException {
            return put(key, value, true);
        }

        private V put(K key, V value, boolean changeSize) throws InterruptedException {
            var varHandle = MethodHandles.arrayElementVarHandle(int[].class);
            int bucketIndex = bucketIndex(key);
            int tries = 0;
            int before;
            while (true) {
                before = (int) varHandle.getAcquire(this.seqLocks, bucketIndex);
                if (before % 2 != 0 || !varHandle.compareAndSet(this.seqLocks, bucketIndex, before, before+1)) {
                    tries++;
                    if (tries > TRIES_THRESHOLD) {
                        throw new InterruptedException();
                    }
                    Thread.yield();
                } else {
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
                    el.value = value;
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
    }

    public static class KeyValue<K, V> {
        public K key;
        public V value;

        KeyValue(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    private void getAllLocks() {
        for (int i = 0; i < buckets.seqLocks.length; i++) {
            VarHandle varHandle = MethodHandles.arrayElementVarHandle(int[].class);
            while (true) {
                int before = (int) varHandle.getAcquire(buckets.seqLocks, i);
                if (before % 2 != 0) {
                    Thread.yield();;
                    continue;
                }
                if (!varHandle.compareAndSet(buckets.seqLocks, i, before, before+1)) {
                    Thread.yield();
                    continue;
                }
                break;
            }
        }
    }

    private void unlockAll() {
        for (int i = 0; i < buckets.seqLocks.length; i++) {
            VarHandle varHandle = MethodHandles.arrayElementVarHandle(int[].class);
            int before = (int) varHandle.get(buckets.seqLocks, i);
            if (!varHandle.compareAndSet(buckets.seqLocks, i, before, before+1)) {
                throw new RuntimeException("Unexpected seqlock state");
            }
        }
    }

    private static class CopyToPartial<K, V> implements CollaborativeTask {
        List<Collection<KeyValue<K, V>>> partialSnapshots;
        Buckets<K, V> buckets;
        int currentIndex;
        int endIndex;

        public CopyToPartial(
                // must be synchronized
                List<Collection<KeyValue<K, V>>> partialSnapshots,
                Buckets<K, V> buckets,
                int startIndex,
                int endIndex
        ) {
            this.partialSnapshots = partialSnapshots;
            this.buckets = buckets;
            this.currentIndex = startIndex;
            this.endIndex = endIndex;
        }

        @Override
        public void start() {
            List<KeyValue<K, V>> list = new ArrayList<>();
            for (; currentIndex < endIndex; currentIndex++) {
                if (buckets.buckets[currentIndex] == null) continue;
                for (var el: buckets.buckets[currentIndex]) {
                    list.add(new KeyValue<>(el.getKey(), el.getValue()));
                }
            }
            partialSnapshots.add(list);
        }
    }

    private static class CopyPartialToSnapshot<K, V> implements CollaborativeTask {
        Collection<KeyValue<K, V>>[] partialSnapshots;
        KeyValue<K, V>[] snapshot;
        int snapshotIndex;
        int currentIndex;
        int endIndex;

        public CopyPartialToSnapshot(
            Collection<KeyValue<K, V>>[] partialSnapshots,
            int beginSnapshotIndex,
            KeyValue<K, V>[] snapshot,
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
                for (var el: partialSnapshots[currentIndex]) {
                    snapshot[snapshotIndex++] = el;
                }
            }
        }
    }

    public KeyValue<K, V>[] snapshot() {
        getAllLocks();


        List<Collection<KeyValue<K, V>>> partialSnapshots = Collections.synchronizedList(new ArrayList<>());

        int delta = 8192;
        int startIndex = 0;
        int endIndex = delta;
        long startTime = System.nanoTime();
        while (endIndex < buckets.buckets.length) {
            CollaborativeTask task = new CopyToPartial<>(
                    partialSnapshots,
                    buckets,
                    startIndex,
                    endIndex
            );
            startIndex += delta;
            endIndex += delta;
            collaborativeQueue.add(task);
        }
        CollaborativeTask taskFinal = new CopyToPartial<>(
                partialSnapshots,
                buckets,
                startIndex,
                buckets.buckets.length
        );
        collaborativeQueue.add(taskFinal);

        int overallSize = size();
        KeyValue<K, V>[] snapshot = new KeyValue[overallSize];
        collaborativeQueue.helpIfNeed();
        collaborativeQueue.waitForFinish();
        long timeExecuted = System.nanoTime() - startTime;
        int curSize = 0;
        int ind = 0;

        Collection<KeyValue<K, V>>[] partialSnapshotsArray = (Collection<KeyValue<K,V>>[]) partialSnapshots.toArray(new Collection[0]);


        for (var part: partialSnapshots) {
            CollaborativeTask task = new CopyPartialToSnapshot<>(
                    partialSnapshotsArray,
                    curSize,
                    snapshot,
                    ind,
                    ++ind
            );
            curSize += part.size();
            collaborativeQueue.add(task);
        }

        collaborativeQueue.helpIfNeed();
        collaborativeQueue.waitForFinish();

        snapshotTimes.putIfAbsent(overallSize, new LinkedList<>());
        snapshotTimes.get(overallSize).add(timeExecuted);

        unlockAll();
        return snapshot;
    }




    private Lock rebuildLock = new ReentrantLock();

    private static class RebuildInitTask<K, V> implements CollaborativeTask {
        Buckets<K,V> newBuckets;
        int currentIndex;
        int endIndex;

        public RebuildInitTask(
                Buckets<K, V> newBuckets,
                int startIndex,
                int endIndex
        ) {
            this.newBuckets = newBuckets;
            this.currentIndex = startIndex;
            this.endIndex = endIndex;
        }

        public void start() {
            while (currentIndex < endIndex) {
                newBuckets.buckets[currentIndex] = new ArrayDeque<>();
                currentIndex++;
            }
        }
    }

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
                            newBuckets.put(el.key, el.value, false);
                            break;
                        } catch (InterruptedException e) {
                            continue;
                        }
                    }
//                    newBuckets.buckets[Math.abs(el.key.hashCode()) % newBuckets.buckets.length].add(el);
                    sizeIncrement++;
                }
                currentIndex++;
            }
            newBuckets.size.getAndAdd(sizeIncrement);
        }

//        public RebuildTask<K, V> split() {
//            // слишком мало осталось сделать -- нет смысла делить
//            if (endIndex - currentIndex < MIN_SPLIT_SIZE) {
//                return null;
//            }
//            int medIndex = (endIndex + currentIndex) / 2;
//            var newTask = new RebuildTask<K, V>(
//                    oldBuckets,
//                    newBuckets,
//                    medIndex,
//                    endIndex
//            );
//            endIndex = medIndex;
//            // за время разделения полностью прошли левую часть
//            if (currentIndex >= endIndex) {
//                throw new RuntimeException("Получили не консистентное состояние");
//            }
//
//            return newTask;
//        }
    }

    private void rebuild() {
        if (rebuildLock.tryLock()) {
            try {
//                System.out.println("Rebuild started");
                getAllLocks();
                var nanoStart = System.nanoTime();

                // создаем новые бакеты
                var newBuckets = new Buckets<K, V>(buckets.buckets.length * 2);

                // начинаем создавать таски на инициализацию
                int delta = 8192;
                int startIndex = 0;
                int endIndex = delta;
//                while (endIndex < newBuckets.buckets.length) {
//                    RebuildInitTask<K, V> task = new RebuildInitTask<>(
//                            newBuckets,
//                            startIndex,
//                            endIndex
//                    );
//                    startIndex += delta;
//                    endIndex += delta;
//                    collaborativeQueue.add(task);
//                }
//                RebuildInitTask<K, V> initTaskFinal = new RebuildInitTask<>(
//                        newBuckets,
//                        startIndex,
//                        newBuckets.buckets.length
//                );
//                collaborativeQueue.add(initTaskFinal);
////                System.out.println("Finish creating tasks");
//                collaborativeQueue.helpIfNeed();
////                System.out.println("Waiting of finish initialization");
//                collaborativeQueue.waitForFinish();

                startIndex = 0;
                endIndex = delta;
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
                collaborativeQueue.helpIfNeed();
                collaborativeQueue.waitForFinish();
                Long timeExecuted = System.nanoTime() - nanoStart;
                buckets = newBuckets;
                rebuildTimes.putIfAbsent(buckets.buckets.length, new LinkedList<>());
                rebuildTimes.get(buckets.buckets.length).add(timeExecuted);
//                System.out.println("Rebuild finished");
//            } catch (InterruptedException ignored) {
            } finally {
                rebuildLock.unlock();
            }
        }
    }

    private void rebuildIfNeed() {
        if (size() > 0.75 * buckets.buckets.length) {
//            System.out.println("Current size before rebuild: " + size());
            rebuild();
//            System.out.println("Current size after  rebuild: " + size());
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
            if (bucket != null) {
                ans += bucket.size();
            }
        }
        return ans;
    }

    public V get(K key) {
        while (true) {
            try {
                return buckets.get(key);
            } catch (InterruptedException ignored) {
                collaborativeQueue.helpIfNeed();
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
                collaborativeQueue.helpIfNeed();
            }
        }
        rebuildIfNeed();
        return result;
    }

    public V remove(K key) {
        throw new RuntimeException("Not implemented yet");
    }
}
