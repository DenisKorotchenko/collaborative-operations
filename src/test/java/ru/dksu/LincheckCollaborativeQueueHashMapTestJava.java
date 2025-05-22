package ru.dksu;

import org.jetbrains.kotlinx.lincheck.annotations.Operation;

public class LincheckCollaborativeQueueHashMapTestJava {
    private CollaborativeQueueHashMap<Integer, Integer> map = new CollaborativeQueueHashMap<Integer, Integer>();

    @Operation
    Integer put(Integer key, Integer value) {
        return map.put(key, value);
    }

    @Operation
    Integer get(Integer key) {
        return map.get(key);
    }

    @Operation
    Integer remove(Integer key) {
        return map.remove(key);
    }

}
