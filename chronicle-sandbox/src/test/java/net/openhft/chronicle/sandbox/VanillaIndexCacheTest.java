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

import org.junit.Test;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import net.openhft.lang.io.IOTools;

import static org.junit.Assert.*;

public class VanillaIndexCacheTest {
    @Test
    public void testIndexFor() throws Exception {
        File dir = new File(System.getProperty("java.io.tmpdir"), "testIndexFor");
        DateCache dateCache = new DateCache("yyyyMMddHHmmss", 1000);
        VanillaIndexCache cache = new VanillaIndexCache(dir.getAbsolutePath(), 10 + 3, dateCache);

        int cycle = (int) (System.currentTimeMillis() / 1000);
        VanillaFile vanillaFile0 = cache.indexFor(cycle, 0, true);
        vanillaFile0.bytes().writeLong(0, 0x12345678);
        File file0 = vanillaFile0.file();
        assertEquals(8 << 10, file0.length());
        assertEquals(0x12345678L, vanillaFile0.bytes().readLong(0));
        vanillaFile0.decrementUsage();

        VanillaFile vanillaFile1 = cache.indexFor(cycle, 1, true);
        File file1 = vanillaFile1.file();
        assertEquals(8 << 10, file1.length());
        vanillaFile1.decrementUsage();
        assertNotEquals(vanillaFile1.file(), vanillaFile0.file());

        VanillaFile vanillaFile2 = cache.indexFor(cycle, 2, true);
        File file2 = vanillaFile2.file();
        assertEquals(8 << 10, file2.length());
        vanillaFile2.decrementUsage();

        assertNotEquals(vanillaFile2.file(), vanillaFile0.file());
        assertNotEquals(vanillaFile2.file(), vanillaFile1.file());
        cache.close();
        assertEquals(0, vanillaFile0.usage());
        assertEquals(0, vanillaFile1.usage());
        assertEquals(0, vanillaFile2.usage());
        // check you can delete after closing.
        assertTrue(file0.delete());
        assertTrue(file1.delete());
        assertTrue(file2.delete());
        assertTrue(file0.getParentFile().delete());
        file0.getParentFile().getParentFile().delete();
    }

    @Test
    public void testConcurrentAppend() throws Exception {
        File dir = new File(System.getProperty("java.io.tmpdir"), "testConcurrentAppend");
        IOTools.deleteDir(dir.getAbsolutePath());

        DateCache dateCache = new DateCache("yyyyMMddHHmmss", 1000);

        // Use a small index file size so that the test frequently generates new index files
        VanillaIndexCache cache = new VanillaIndexCache(dir.getAbsolutePath(), 5, dateCache);

        int cycle = (int) (System.currentTimeMillis() / 1000);
        final int numberOfTasks = 2;
        final int count = 1000;

        // Create and start concurrent tasks that append to the index
        final List<IndexAppendTask> tasks = new ArrayList<>();
        final List<Thread> threads = new ArrayList<>();
        long startIndex = count;
        for (int i = 0; i < numberOfTasks; i++) {
            final long endIndex = startIndex + count;
            final IndexAppendTask task = new IndexAppendTask(cache, cycle, startIndex, endIndex);
            tasks.add(task);
            final Thread thread = new Thread(task, "task" + i);
            threads.add(thread);
            thread.start();
            startIndex = endIndex;
        }

        // Wait for all tasks to finish
        for (Thread thread : threads) {
            thread.join();
        }

        // Fail if any tasks failed
        for (IndexAppendTask task : tasks) {
            task.assertIfFailed();
        }

        cache.close();
        IOTools.deleteDir(dir.getAbsolutePath());
    }


    private static class IndexAppendTask implements Runnable {
        private final VanillaIndexCache cache;
        private final int cycle;
        private final long start;
        private final long end;

        private volatile AssertionError failure;

        private IndexAppendTask(final VanillaIndexCache cache, final int cycle, final long start, final long end) {
            this.cache = cache;
            this.cycle = cycle;
            this.start = start;
            this.end = end;
        }

        @Override
        public void run() {
            long index = start;
            while (failure == null && index < end) {
                try {
                    cache.append(cycle, index, false);
                    index++;
                } catch (Throwable e) {
                    this.failure = new AssertionError("Failed append for index " + index, e);
                }
            }
            System.out.println(String.format("Task %s for %s to %s", (failure == null) ? "SUCCESS" : "FAIL", start, index - 1));
        }

        public void assertIfFailed() throws AssertionError {
            if (failure != null) {
                throw failure;
            }
        }
    }

}
