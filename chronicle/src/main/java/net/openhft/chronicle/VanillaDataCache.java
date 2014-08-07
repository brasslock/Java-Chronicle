/*
 * Copyright 2014 Higher Frequency Trading
 * <p/>
 * http://www.higherfrequencytrading.com
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle;

import net.openhft.lang.io.VanillaMappedBytes;
import net.openhft.lang.io.VanillaMappedCache;
import net.openhft.lang.model.constraints.NotNull;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public class VanillaDataCache implements Closeable {
    private static final String FILE_NAME_PREFIX = "data-";

    private final String basePath;
    private final DataKey key = new DataKey();
    private final int blockBits;
    private final DateCache dateCache;
    private final VanillaMappedCache<DataKey> cache;

    public VanillaDataCache(@NotNull String basePath, int blockBits, @NotNull DateCache dateCache) {
        this(basePath, blockBits, dateCache, VanillaChronicleConfig.DEFAULT);
    }

    public VanillaDataCache(@NotNull String basePath, int blockBits, @NotNull DateCache dateCache, @NotNull VanillaChronicleConfig config) {
        this(basePath, blockBits, dateCache, config.dataCacheCapacity(), config.cleanupOnClose());
    }

    public VanillaDataCache(@NotNull String basePath, int blockBits, @NotNull DateCache dateCache, int capacity) {
        this(basePath, blockBits, dateCache, capacity, false);
    }

    public VanillaDataCache(@NotNull String basePath, int blockBits, @NotNull DateCache dateCache, int capacity, boolean cleanupOnClose) {
        this.basePath = basePath;
        this.blockBits = blockBits;
        this.dateCache = dateCache;
        this.cache = new VanillaMappedCache<DataKey>(capacity, true, cleanupOnClose);
    }


    public File fileFor(int cycle, int threadId, int dataCount, boolean forWrite) throws IOException {
        return new File(
            new File(basePath, dateCache.formatFor(cycle)),
            FILE_NAME_PREFIX + threadId + "-" + dataCount);
    }

    public File fileFor(int cycle, int threadId) throws IOException {
        String cycleStr = dateCache.formatFor(cycle);
        String cyclePath = basePath + "/" + cycleStr;
        String dataPrefix = FILE_NAME_PREFIX + threadId + "-";
        int maxCount = 0;
        File[] files = new File(cyclePath).listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.getName().startsWith(dataPrefix)) {
                    int count = Integer.parseInt(file.getName().substring(dataPrefix.length()));
                    if (maxCount < count)
                        maxCount = count;
                }
            }
        }

        return fileFor(cycle, threadId, maxCount, true);
    }

    public synchronized VanillaMappedBytes dataFor(int cycle, int threadId, int dataCount, boolean forWrite) throws IOException {
        key.cycle = cycle;
        key.threadId = threadId;
        key.dataCount = dataCount;

        VanillaMappedBytes vmb = this.cache.get(key);
        if(vmb == null) {
            vmb = this.cache.put(
                key.clone(),
                VanillaChronicleUtils.mkFiles(
                    basePath,
                    dateCache.formatFor(cycle),
                    FILE_NAME_PREFIX + threadId + "-" + dataCount,
                    forWrite),
                1L << blockBits,
                dataCount);
        }

        vmb.reserve();

        return vmb;
    }

    private void findEndOfData(final VanillaMappedBytes buffer) {
        for (int i = 0, max = 1 << blockBits; i < max; i += 4) {
            int len = buffer.readInt(buffer.position());
            if (len == 0) {
                return;
            }
            int len2 = nextWordAlignment(~len);
            if (len2 < 0) {
                throw new IllegalStateException("Corrupted length " + Integer.toHexString(len));
                //throw new IllegalStateException("Corrupted length in " + vanillaFile.file() + " " + Integer.toHexString(len));
            }
            buffer.position(buffer.position() + len2 + 4);
        }
        throw new AssertionError();
    }

    int nextWordAlignment(int len) {
        return (len + 3) & ~3;
    }

    @Override
    public synchronized void close() {
        this.cache.close();
    }

    /**
     * Find the count for the next data file to be written for a specific thread.
     */
    public int findNextDataCount(int cycle, int threadId) throws IOException {
        final String cycleStr = dateCache.formatFor(cycle);
        final String cyclePath = basePath + "/" + cycleStr;
        final String dataPrefix = FILE_NAME_PREFIX + threadId + "-";

        int maxCount = -1;
        final File[] files = new File(cyclePath).listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.getName().startsWith(dataPrefix)) {
                    final int count = Integer.parseInt(file.getName().substring(dataPrefix.length()));
                    if (maxCount < count)
                        maxCount = count;
                }
            }
        }
        // Move to the next data file
        return maxCount + 1;
    }

    public void checkCounts(int min, int max) {
        this.cache.checkCounts(min,max);
    }


    static class DataKey implements Cloneable {
        int cycle;
        int threadId;
        int dataCount;

        @Override
        public int hashCode() {
            return threadId * 10191 + cycle * 17 + dataCount;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof DataKey)) {
                return false;
            }

            DataKey key = (DataKey) obj;
            return dataCount == key.dataCount && threadId == key.threadId && cycle == key.cycle;
        }

        @Override
        protected DataKey clone() {
            try {
                return (DataKey) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public String toString() {
            return "DataKey ["
                + "cycle="     + cycle     + ","
                + "threadId="  + threadId  + ","
                + "dataCount=" + dataCount + "]";
        }
    }
}
