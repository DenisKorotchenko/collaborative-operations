package ru.dksu

import org.jetbrains.kotlinx.lincheck.annotations.Operation
import org.jetbrains.kotlinx.lincheck.annotations.Param
import org.jetbrains.kotlinx.lincheck.check
import org.jetbrains.kotlinx.lincheck.paramgen.IntGen
import org.jetbrains.kotlinx.lincheck.strategy.managed.modelchecking.ModelCheckingOptions
import org.jetbrains.kotlinx.lincheck.strategy.stress.StressOptions
import org.junit.jupiter.api.Test

@Param(name = "key", gen = IntGen::class, conf = "1:20")
class LincheckCollaborativeQueueHashMapTest {
    private val map = CollaborativeQueueHashMap<Int, Int>()

    @Operation
    fun put(@Param(name = "key") key: Int, value: Int): Int? {
        return map.put(key, value)
    }

    @Operation
    fun get(key: Int): Int? {
        return map[key]
    }

    @Operation
    fun reduce(): Int =
        map.reduce(
            0,
            { r, el -> el + r },
        )

    @Operation
    fun snapshot(): Set<Pair<Int, Int>> {
        val res = map.snapshot()
        return res.map { it.key to it.value }.toSet()
    }

    @Operation
    fun remove(@Param(name = "key") key: Int): Int? {
        return map.remove(key)
    }

    @Test
    fun stress() = StressOptions()
        .actorsBefore(20)
        .actorsPerThread(4)
        .actorsAfter(1)
        .iterations(100)
        .minimizeFailedScenario(false)
        .check(this::class)

    @Test
    fun withHashMap() = StressOptions()
        .actorsBefore(20)
        .actorsPerThread(10)
        .actorsAfter(1)
        .iterations(100)
        .minimizeFailedScenario(false)
        .sequentialSpecification(SequentialHashMap::class.java)
        .check(this::class)

    class SequentialHashMap {
        private val h = mutableMapOf<Int, Int>()

        fun put(key: Int, value: Int) = h.put(key, value)
        fun get(key: Int) = h.get(key)
        fun snapshot() = h.entries.map { it.key to it.value }.toSet()
        fun remove(key: Int) = h.remove(key)
        fun reduce() = h.values.sum()
    }
}