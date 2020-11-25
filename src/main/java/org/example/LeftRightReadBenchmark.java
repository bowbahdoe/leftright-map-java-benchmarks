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

import dev.mccue.left_right.LeftRightMap;
import dev.mccue.left_right.LeftRightMap.Reader;
import dev.mccue.left_right.LeftRightMap.Writer;
import site.ycsb.generator.NumberGenerator;
import site.ycsb.generator.ScrambledZipfianGenerator;

@State(Scope.Benchmark)
public class LeftRightReadBenchmark {
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
                writer.put(ints[i], Boolean.TRUE);
                concurrentHashMap.put(ints[i], Boolean.TRUE);
                synchronizedMap.put(ints[i], Boolean.TRUE);
            }
        }
    }

    @Benchmark @Threads(8)
    public Boolean leftRightMap(ThreadState threadState) {
        if (threadState.reader == null) {
            threadState.reader = leftRightMap.readerFactory().createReader();
        }
        return threadState.reader.get(ints[threadState.index++ & MASK]);
    }

    @Benchmark @Threads(8)
    public Boolean threadLocalLeftRightMap(ThreadState threadState) {
        return leftRightMap.threadSafeReader().get(ints[threadState.index++ & MASK]);
    }


    @Benchmark @Threads(8)
    public Boolean concurrentHashMap(ThreadState threadState) {
        return concurrentHashMap.get(ints[threadState.index++ & MASK]);
    }

    @Benchmark @Threads(8)
    public Boolean synchronizedMap(ThreadState threadState) {
        return synchronizedMap.get(ints[threadState.index++ & MASK]);
    }
}