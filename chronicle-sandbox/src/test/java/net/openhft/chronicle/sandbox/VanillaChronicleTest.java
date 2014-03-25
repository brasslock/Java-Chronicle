/*
 * Copyright 2013 Peter Lawrey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.sandbox;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import net.openhft.lang.io.IOTools;

import org.junit.ComparisonFailure;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class VanillaChronicleTest {
    private static final int N_THREADS = 4;

    @Test
    public void testAppend() throws IOException {
        final int RUNS = 1000;
        String baseDir = System.getProperty("java.io.tmpdir") + "/testAppend";
        VanillaChronicleConfig config = new VanillaChronicleConfig();
        config.defaultMessageSize(128);
        config.indexBlockSize(1024);
        config.dataBlockSize(1024);
        VanillaChronicle chronicle = new VanillaChronicle(baseDir, config);
        chronicle.clear();
        ExcerptAppender appender = chronicle.createAppender();
        for (int i = 0; i < RUNS; i++) {
//            System.err.println("i: " + i);
//            if (i == 256)
//                Thread.yield();
            appender.startExcerpt();
            appender.append(1000000000 + i);
            appender.finish();
            chronicle.checkCounts(1, 2);
        }
        chronicle.close();
        chronicle.clear();
    }

    @Test
    public void testAppend4() throws IOException, InterruptedException {
        final int RUNS = 20000;
        String baseDir = System.getProperty("java.io.tmpdir") + "/testAppend4";
        VanillaChronicleConfig config = new VanillaChronicleConfig();
        config.defaultMessageSize(128);
//        config.indexBlockSize(1024);
//        config.dataBlockSize(1024);
        long start = System.nanoTime();
        final VanillaChronicle chronicle = new VanillaChronicle(baseDir, config);
        chronicle.clear();
        ExecutorService es = Executors.newFixedThreadPool(N_THREADS);
        for (int t = 0; t < N_THREADS; t++) {
            final int finalT = t;
            es.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        ExcerptAppender appender = chronicle.createAppender();
                        for (int i = 0; i < RUNS; i++) {
                            appender.startExcerpt();
                            appender.append(finalT).append("/").append(i).append('\n');
                            appender.finish();
                            Thread.yield();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        es.shutdown();
        es.awaitTermination(2, TimeUnit.SECONDS);
        long time = System.nanoTime() - start;
        chronicle.close();
        chronicle.clear();
        System.out.printf("Took an average of %.1f us per entry%n", time / 1e3 / (RUNS * N_THREADS));
    }

    @Test
    public void testTailer() throws IOException {
        final int RUNS = 1000000; // 5000000;
        String baseDir = System.getProperty("java.io.tmpdir") + "/" + "testTailer";
        VanillaChronicleConfig config = new VanillaChronicleConfig();
        config.defaultMessageSize(128);
        config.indexBlockSize(256 << 10);
        config.dataBlockSize(512 << 10);
        VanillaChronicle chronicle = new VanillaChronicle(baseDir, config);
        chronicle.clear();
        VanillaChronicle chronicle2 = new VanillaChronicle(baseDir, config);
        try {
            ExcerptAppender appender = chronicle.createAppender();
            ExcerptTailer tailer = chronicle2.createTailer();

            assertEquals(-1L, tailer.index());
            for (int i = 0; i < RUNS; i++) {
//                if ((i & 65535) == 0)
//                    System.err.println("i: " + i);
//                if (i == 88000)
//                    Thread.yield();
                assertFalse(tailer.nextIndex());
                appender.startExcerpt();
                int value = 1000000000 + i;
                appender.append(value).append(' ');
                appender.finish();
                chronicle.checkCounts(1, 2);
                assertTrue("i: " + i, tailer.nextIndex());
                chronicle2.checkCounts(1, 2);
                assertTrue("i: " + i + " remaining: " + tailer.remaining(), tailer.remaining() > 0);
                assertEquals("i: " + i, value, tailer.parseLong());
                assertEquals("i: " + i, 0, tailer.remaining());
                tailer.finish();
                chronicle2.checkCounts(1, 2);
            }
        } finally {
            chronicle2.close();
            chronicle.clear();
        }
    }


    @Test
    public void testTailerPerf() throws IOException {
        final int WARMUP = 50000;
        final int RUNS = 5000000;
        String baseDir = System.getProperty("java.io.tmpdir") + "/testTailerPerf";
        VanillaChronicle chronicle = new VanillaChronicle(baseDir);
        chronicle.clear();
        try {
            ExcerptAppender appender = chronicle.createAppender();
            ExcerptTailer tailer = chronicle.createTailer();
            long start = 0;
            assertEquals(-1L, tailer.index());
            for (int i = -WARMUP; i < RUNS; i++) {
                if (i == 0)
                    start = System.nanoTime();
                boolean condition0 = tailer.nextIndex();
                if (condition0)
                    assertFalse("i: " + i, condition0);
                appender.startExcerpt();
                int value = 1000000000 + i;
                appender.append(value).append(' ');
                appender.finish();
                boolean condition = tailer.nextIndex();
                long actual = tailer.parseLong();
                if (i < 0) {
                    assertTrue("i: " + i, condition);
                    assertEquals("i: " + i, value, actual);
                }
                tailer.finish();
            }
            long time = System.nanoTime() - start;
            System.out.printf("Average write/read times was %.3f us%n", time / RUNS / 1e3);
        } finally {
            chronicle.close();
            chronicle.clear();
        }
    }


    @Test
    public void testTailerPerf2() throws IOException, InterruptedException {
        final int WARMUP = 100000;
        final int RUNS = 4000000;
        final int BYTES = 96;
        String baseDir = System.getProperty("java.io.tmpdir") + "/testTailerPerf";
        final VanillaChronicle chronicle = new VanillaChronicle(baseDir);
        chronicle.clear();
        try {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = -WARMUP; i < RUNS; i++) {
                        ExcerptTailer tailer = null;
                        try {
                            tailer = chronicle.createTailer();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        while (!tailer.nextIndex()) ;
                        long actual = -1;
                        for (int j = 0; j < BYTES; j += 8)
                            actual = tailer.readLong();
                        if (i < 0) {
                            int value = 1000000000 + i;
                            assertEquals("i: " + i, value, actual);
                        }
                        tailer.finish();
                    }

                }
            });
            t.start();
            ExcerptAppender appender = chronicle.createAppender();
            long start = 0;
            for (int i = -WARMUP; i < RUNS; i++) {
                if (i == 0)
                    start = System.nanoTime();
                appender.startExcerpt();
                int value = 1000000000 + i;
                for (int j = 0; j < BYTES; j += 8)
                    appender.writeLong(value);
                appender.finish();
            }
            t.join();
            long time = System.nanoTime() - start;
            System.out.printf("Average write/read times was %.3f us%n", time / RUNS / 1e3);
        } finally {
            chronicle.close();
            chronicle.clear();
        }
    }

    @Test
    public void testAppenderTailer() throws IOException {
        String basepath = System.getProperty("java.io.tmpdir") + "/test-appender-tailer";

        VanillaChronicle writer = new VanillaChronicle(basepath);
        writer.clear();

        ExcerptAppender appender = writer.createAppender();

        for(long i=0;i<3;i++) {
            appender.startExcerpt();
            appender.writeLong(i);
            appender.finish();
        }

        {
            VanillaChronicle reader = new VanillaChronicle(basepath);
            ExcerptTailer tailer = reader.createTailer();

            for(long i=0;i<3;i++) {
                assertTrue(tailer.nextIndex());
                assertEquals(i,tailer.readLong());
                tailer.finish();
            }

            tailer.close();
            reader.close();
        }

        {
            VanillaChronicle reader = new VanillaChronicle(basepath);
            ExcerptTailer tailer = reader.createTailer().toStart();

            for(long i=0;i<3;i++) {
                assertTrue(tailer.nextIndex());
                assertEquals(i,tailer.readLong());
                tailer.finish();
            }

            tailer.close();
            reader.close();
        }

        appender.close();
        writer.close();
    }

    @Test
    public void testTailerToStart() throws IOException {
        String basepath = System.getProperty("java.io.tmpdir") + "/test-tailer-tostart";

        VanillaChronicle chronicle  = new VanillaChronicle(basepath);
        chronicle.clear();

        ExcerptAppender appender = chronicle.createAppender();
        ExcerptTailer tailer = null;

        for(long i=0;i<3;i++) {
            appender.startExcerpt();
            appender.writeLong(i);
            appender.finish();
        }

        appender.close();

        // test a vanilla tailer, no rewind
        tailer = chronicle.createTailer();
        for(long i=0;i<3;i++) {
            assertTrue(tailer.nextIndex());
            assertEquals(i, tailer.readLong());
            tailer.finish();
        }

        tailer.close();

        // test a vanilla tailer, rewind
        tailer = chronicle.createTailer().toStart();
        for(long i=0;i<3;i++) {
            assertTrue(tailer.nextIndex());
            assertEquals(i, tailer.readLong());
            tailer.finish();
        }

        tailer.close();
        chronicle.close();
    }

    @Test
    public void testTailerToEnd() throws IOException {
        String basepath = System.getProperty("java.io.tmpdir") + "/test-tailer-toend";

        VanillaChronicle chronicle  = new VanillaChronicle(basepath);
        chronicle.clear();

        ExcerptAppender appender = chronicle.createAppender();
        for(long i=0;i<3;i++) {
            appender.startExcerpt();
            appender.writeLong(i);
            appender.finish();
        }

        appender.close();

        // test a vanilla tailer, wind to end
        ExcerptTailer tailer = chronicle.createTailer().toEnd();
        assertEquals(2,tailer.readLong());
        assertFalse(tailer.nextIndex());

        tailer.close();
        chronicle.close();
    }

    @Test
    public void testTailerEndStart() throws IOException {
        String basepath = System.getProperty("java.io.tmpdir") + "/test-tailer-endstart";

        VanillaChronicle chronicle  = new VanillaChronicle(basepath);
        chronicle.clear();

        ExcerptAppender appender = chronicle.createAppender();
        ExcerptTailer tailer = null;

        for(long i=0;i<3;i++) {
            appender.startExcerpt();
            appender.writeLong(i);
            appender.finish();
        }

        appender.close();

        // test a vanilla tailer, wind to end
        tailer = chronicle.createTailer().toEnd();
        assertEquals(2,tailer.readLong());
        assertFalse(tailer.nextIndex());

        tailer.finish();
        tailer.close();

        // test a vanilla tailer, rewind
        tailer = chronicle.createTailer().toStart();
        for(long i=0;i<3;i++) {
            assertTrue(tailer.nextIndex());
            assertEquals(i, tailer.readLong());
            tailer.finish();
        }

        tailer.close();

        chronicle.close();
    }

    @Test
    public void testConcurrentAppend() throws Exception {
        String basepath = System.getProperty("java.io.tmpdir") + "/testConcurrentAppend";

        // Create with small data and index sizes so that the test frequently generates new files
        final VanillaChronicleConfig config = new VanillaChronicleConfig()
                .dataBlockSize(64)
                .indexBlockSize(64);
        VanillaChronicle chronicle = new VanillaChronicle(basepath, config);
        chronicle.clear();

        final int numberOfTasks = 2;
        final int countPerTask = 1000;

        // Create tasks that append to the index
        final List<Callable<Void>> tasks = new ArrayList<Callable<Void>>();
        int nextValue = countPerTask;
        for (int i = 0; i < numberOfTasks; i++) {
            final int endValue = nextValue + countPerTask;
            tasks.add(createAppendTask(chronicle, nextValue, endValue));
            nextValue = endValue;
        }

        // Execute tasks using a thread per task
        TaskExecutionUtil.executeConcurrentTasks(tasks, 30000L);

        // Verify that all values have been written
        final ExcerptTailer tailer = chronicle.createTailer();
        final Set<String> values = readAvailableValues(tailer);
        assertEquals(createRangeDataSet(countPerTask, nextValue), values);
        tailer.close();

        chronicle.close();
    }

    @Test
    public void testReadsAndWritesAcrossCycles() throws Exception {

        for (int i = 0; i < 100; i++) {
            System.out.println("Iteration = " + i);

            String basepath = System.getProperty("java.io.tmpdir") + "/testRead1";
//            String basepath = "/data/ross/testCycle";
//            IOTools.deleteDir(basepath);

            // Create with small data and index sizes so that the test frequently generates new files
            final VanillaChronicleConfig config = new VanillaChronicleConfig()
                    .cycleLength(1000)  // 1 second
                    .entriesPerCycle(1L << 20)  // avoid overflow of the entry indexes
                    .cycleFormat("yyyyMMddHHmmss")
                    .dataBlockSize(128)
                    .indexBlockSize(64);
            VanillaChronicle chronicle = new VanillaChronicle(basepath, config);
//            chronicle.clear();

//            final ExcerptAppender appender = chronicle.createAppender();
            final ExcerptTailer tailer = chronicle.createTailer();

//            appendValues(appender, 1, 200);
//
//            Ensure the appender writes in another cycle from the initial writes
//            Thread.sleep(1000L);

            final Set<String> actual = readAvailableValues(tailer);
            final Set<String> expected = createRangeDataSet(1, 200);
            if (!expected.equals(actual)) {
                System.err.println("ERROR: " + actual);
                Thread.sleep(10000L);

                final Set<String> following = readAvailableValues(tailer);
                System.err.println("FOLLOWING: " + following);

                throw new RuntimeException("failed");
            }

//            appendValues(appender, 20, 40);
//
//            Thread.sleep(1000L);
//
//            assertEquals(createRangeDataSet(20, 40), readAvailableValues(tailer));
//
//            appendValues(appender, 40, 60);
//
//            Thread.sleep(1000L);
//
//            assertEquals(createRangeDataSet(40, 60), readAvailableValues(tailer));

//
//            // Verify that all values are read by the tailer
//            final ExcerptTailer tailer = chronicle.createTailer();
//            final Set<String> actual = readAvailableValues(tailer);
//            assertEquals(createRangeDataSet(1, 40), actual);
//
//        // Verify that the tailer reads no new data from a new cycle
//        Thread.sleep(2000L);
//        assertTrue(!tailer.nextIndex());
//        assertTrue(!tailer.nextIndex());  // Throws java.lang.NullPointerException
//
//        // Append data in this new cycle
//        appendValues(appender, 41, 60);
//
//        // Verify that the tailer can read the new data
//        assertEquals(createRangeDataSet(41, 60), readAvailableValues(tailer));

//        appender.close();
            tailer.close();
            chronicle.close();
        }
    }

    private static void appendValues(final ExcerptAppender appender, final long startValue, final long endValue) {
        long counter = startValue;
        while (counter < endValue) {
            appender.startExcerpt(20);
            appender.writeUTF("data-" + counter);
            appender.finish();
            counter++;
        }
    }

    private static Set<String> readAvailableValues(final ExcerptTailer tailer) {
        final Set<String> values = new TreeSet<String>();
        while (tailer.nextIndex()) {
            final String value = tailer.readUTF();
            System.out.println(String.format("## Read '%s'", value));
            values.add(value);
        }
        return values;
    }

    private static Set<String> createRangeDataSet(final long start, final long end) {
        final Set<String> values = new TreeSet<String>();
        long counter = start;
        while (counter < end) {
            values.add("data-" + counter);
            counter++;
        }
        return values;
    }

    private static Callable<Void> createAppendTask(final VanillaChronicle chronicle, final long startValue, final long endValue) {
        return new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                final ExcerptAppender appender = chronicle.createAppender();
                try {
                    appendValues(appender, startValue, endValue);
                } finally {
                    appender.close();
                }
                return null;
            }
        };
    }

}
