package org.example;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Group;

import org.openjdk.jmh.annotations.GroupThreads;

import dev.mccue.left_right.LeftRightMap;
import dev.mccue.left_right.LeftRightMap.Reader;
import dev.mccue.left_right.LeftRightMap.Writer;
import site.ycsb.generator.NumberGenerator;
import site.ycsb.generator.ScrambledZipfianGenerator;

@State(Scope.Benchmark)
public class LeftRightReadWriteBenchmark {
    private static final int SIZE = (2 << 14);
    private static final int MASK = SIZE - 1;
    private static final int ITEMS = SIZE / 3;

    LeftRightMap<Integer, Boolean> leftRightMap;
    ConcurrentHashMap<Integer, Boolean> concurrentHashMap;
    Map<Integer, Boolean> synchronizedMap;
    Integer[] ints;

    @State(Scope.Thread)
    public static class ThreadState {
        static final Random random = new Random();
        Reader<Integer, Boolean> reader;
        int index = random.nextInt();
    }

    @Setup
    public void setup() {
        ints = new Integer[SIZE];
        concurrentHashMap = new ConcurrentHashMap<>();
        synchronizedMap = Collections.synchronizedMap(new HashMap<>());
        leftRightMap = LeftRightMap.create();

        // Populate with a realistic access distribution
        NumberGenerator generator = new ScrambledZipfianGenerator(ITEMS);
        try (Writer<Integer, Boolean> writer = leftRightMap.writer()) {
            for (int i = 0; i < SIZE; i++) {
                ints[i] = generator.nextValue().intValue();
            }
        }
    }

    // Left Right
    @Benchmark @GroupThreads(8)
    @Group("LeftRightMap")
    public Boolean leftRightMapRead(ThreadState threadState) {
        if (threadState.reader == null) {
            threadState.reader = leftRightMap.readerFactory().createReader();
        }
        return threadState.reader.get(ints[threadState.index++ & MASK]);
    }

    @Benchmark @GroupThreads(1)
    @Group("LeftRightMap")
    public Boolean leftRightMapWrite(ThreadState threadState) {
        try (Writer<Integer, Boolean> writer = leftRightMap.writer()) {
            writer.put(ints[threadState.index++ & MASK], Boolean.TRUE);
        }

        return Boolean.TRUE;
    }

    // Concurrent Hash Map
    @Benchmark @GroupThreads(8)
    @Group("ConcurrentHashMap")
    public Boolean concurrentHashMapRead(ThreadState threadState) {
        return concurrentHashMap.get(ints[threadState.index++ & MASK]);
    }

    @Benchmark @GroupThreads(1)
    @Group("ConcurrentHashMap")
    public Boolean concurrentHashMapWrite(ThreadState threadState) {
        return concurrentHashMap.put(ints[threadState.index++ & MASK], Boolean.TRUE);
    }

    // Left Write w/ Thread Locals
    @Benchmark @GroupThreads(8)
    @Group("ThreadLocalLeftRightMap")
    public Boolean threadLocalLeftRightMapRead(ThreadState threadState) {
        return leftRightMap.threadSafeReader().get(ints[threadState.index++ & MASK]);
    }

    @Benchmark @GroupThreads(1)
    @Group("ThreadLocalLeftRightMap")
    public Boolean threadLocalLeftRightMapWrite(ThreadState threadState) {
        try (Writer<Integer, Boolean> writer = leftRightMap.writer()) {
            writer.put(ints[threadState.index++ & MASK], Boolean.TRUE);
        }

        return Boolean.TRUE;
    }

    // Synchronized Hash Map

    @Benchmark @Threads(8)
    @Group("SynchronizedMap")
    public Boolean synchronizedMapRead(ThreadState threadState) {
        return synchronizedMap.get(ints[threadState.index++ & MASK]);
    }

    @Benchmark @Threads(1)
    @Group("SynchronizedMap")
    public Boolean synchronizedMapWrite(ThreadState threadState) {
        return synchronizedMap.get(ints[threadState.index++ & MASK]);
    }
}