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

package net.openhft.chronicle;

import net.openhft.affinity.AffinitySupport;
import net.openhft.lang.Maths;
import net.openhft.lang.io.IOTools;
import net.openhft.lang.io.NativeBytes;
import net.openhft.lang.io.VanillaMappedBytes;
import net.openhft.lang.io.serialization.BytesMarshallerFactory;
import net.openhft.lang.io.serialization.impl.VanillaBytesMarshallerFactory;
import net.openhft.lang.model.constraints.NotNull;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static net.openhft.chronicle.VanillaChronicleConfig.INDEX_DATA_OFFSET_BITS;
import static net.openhft.chronicle.VanillaChronicleConfig.INDEX_DATA_OFFSET_MASK;
import static net.openhft.chronicle.VanillaChronicleConfig.THREAD_ID_MASK;

/**
 * Created by peter
 */
public class VanillaChronicle implements Chronicle {
    private static final Logger LOG = LoggerFactory.getLogger(VanillaChronicle.class);

    private final AtomicLong appenderCount = new AtomicLong(0);

    private final String name;
    private final String basePath;
    private final VanillaChronicleConfig config;
    private final ThreadLocal<WeakReference<BytesMarshallerFactory>> marshallersCache;
    private final ThreadLocal<WeakReference<VanillaTailer>> tailerCache;
    private final ThreadLocal<WeakReference<VanillaAppender>> appenderCache;
    private final VanillaIndexCache indexCache;
    private final VanillaDataCache dataCache;
    private final int indexBlockLongsBits, indexBlockLongsMask;
    private final int dataBlockSizeBits, dataBlockSizeMask;
    private final int entriesForCycleBits;
    private final long entriesForCycleMask;

    //    private volatile int cycle;
    private volatile long lastWrittenIndex;
    private volatile boolean closed = false;

    public VanillaChronicle(String basePath) {
        this(basePath, VanillaChronicleConfig.DEFAULT);
    }

    public VanillaChronicle(String basePath, VanillaChronicleConfig config) {
        this.marshallersCache = new ThreadLocal<WeakReference<BytesMarshallerFactory>>();
        this.tailerCache = new ThreadLocal<WeakReference<VanillaTailer>>();
        this.appenderCache = new ThreadLocal<WeakReference<VanillaAppender>>();
        this.basePath = basePath;
        this.config = config;
        this.name = new File(basePath).getName();

        DateCache dateCache = new DateCache(config.cycleFormat(), config.cycleLength());
        int indexBlockSizeBits = Maths.intLog2(config.indexBlockSize());
        int indexBlockSizeMask = -1 >>> -indexBlockSizeBits;

        this.indexCache = new VanillaIndexCache(basePath, indexBlockSizeBits, dateCache, config);
        this.indexBlockLongsBits = indexBlockSizeBits - 3;
        this.indexBlockLongsMask = indexBlockSizeMask >>> 3;

        this.dataBlockSizeBits = Maths.intLog2(config.dataBlockSize());
        this.dataBlockSizeMask = -1 >>> -dataBlockSizeBits;
        this.dataCache = new VanillaDataCache(basePath, dataBlockSizeBits, dateCache, config);

        this.entriesForCycleBits = Maths.intLog2(config.entriesPerCycle());
        this.entriesForCycleMask = -1L >>> -entriesForCycleBits;
    }

    void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException(basePath + " is closed");
        }
    }

    @Override
    public String name() {
        return name;
    }

    public int getEntriesForCycleBits() {
        return entriesForCycleBits;
    }

    @NotNull
    @Override
    public Excerpt createExcerpt() throws IOException {
        return new VanillaExcerpt();
    }

    BytesMarshallerFactory acquireBMF() {
        WeakReference<BytesMarshallerFactory> bmfRef = marshallersCache.get();
        BytesMarshallerFactory bmf = null;
        if (bmfRef != null)
            bmf = bmfRef.get();
        if (bmf == null) {
            bmf = createBMF();
            marshallersCache.set(new WeakReference<BytesMarshallerFactory>(bmf));
        }
        return bmf;
    }

    BytesMarshallerFactory createBMF() {
        return new VanillaBytesMarshallerFactory();
    }

    /**
     * This method returns the very last index in the chronicle.  Not to be confused with lastWrittenIndex(),
     * this method returns the actual last index by scanning the underlying data even the appender has not
     * been activated.
     * @return The last index in the file
     */
    public long lastIndex() {
        int cycle = (int) indexCache.lastCycle();
        int lastIndexCount = indexCache.lastIndexFile(cycle, -1);
        if (lastIndexCount >= 0) {
            try {
                final VanillaMappedBytes buffer = indexCache.indexFor(cycle, lastIndexCount, false);
                final long indices = VanillaIndexCache.countIndices(buffer);
                buffer.release();

                final long indexEntryNumber = (indices > 0) ? indices - 1 : 0;
                return (((long) cycle) << entriesForCycleBits) + (((long) lastIndexCount) << indexBlockLongsBits) + indexEntryNumber;

            } catch (IOException e) {
                throw new AssertionError(e);
            }
        } else {
            return -1;
        }
    }

    @NotNull
    @Override
    public ExcerptTailer createTailer() throws IOException {
        WeakReference<VanillaTailer> ref = tailerCache.get();
        VanillaTailer tailer = null;
        if (ref != null) {
            tailer = ref.get();
            if (tailer != null && tailer.unmapped()) {
                tailer = null;
            }
        }

        if (tailer == null) {
            tailer = createTailer0();
            tailerCache.set(new WeakReference<VanillaTailer>(tailer));
        }
        return tailer;
    }

    private VanillaTailer createTailer0() {
        return new VanillaTailer();
    }

    @NotNull
    @Override
    public VanillaAppender createAppender() throws IOException {
        WeakReference<VanillaAppender> ref = appenderCache.get();
        VanillaAppender appender = null;
        if (ref != null) {
            appender = ref.get();
            if (appender != null && appender.unmapped()) {
                appender = null;
            }
        }
        if (appender == null) {
            appender = createAppender0();
            appenderCache.set(new WeakReference<VanillaAppender>(appender));
        }
        return appender;
    }

    private VanillaAppender createAppender0() {
        return new VanillaAppender();
    }

    @Override
    public long lastWrittenIndex() {
        return lastWrittenIndex;
    }

    @Override
    public long size() {
        return lastWrittenIndex + 1;
    }

    @Override
    public void close() {
        closed = true;
        indexCache.close();
        dataCache.close();
    }

    @Override
    public void clear() {
        indexCache.close();
        dataCache.close();
        IOTools.deleteDir(basePath);
    }

    public void checkCounts(int min, int max) {
        indexCache.checkCounts(min, max);
        dataCache.checkCounts(min, max);
    }

    private abstract class AbstractVanillaExcerptCommon extends NativeBytes implements ExcerptCommon {
        protected VanillaMappedBytes indexBytes;
        protected VanillaMappedBytes dataBytes;

        public AbstractVanillaExcerptCommon() {
            super(acquireBMF(), NO_PAGE, NO_PAGE, null);
        }

        @Override
        public boolean wasPadding() {
            return false;
        }

        public boolean unmapped() {
            return (indexBytes == null || dataBytes == null) || indexBytes.unmapped() && dataBytes.unmapped();
        }

        protected int cycle() {
            return (int) (System.currentTimeMillis() / config.cycleLength());
        }

        /**
         * Return the last index written by the appender.
         * <p/>
         * This may not be the actual last index in the Chronicle which can be
         * found from lastIndex().
         */
        @Override
        public long lastWrittenIndex() {
            return VanillaChronicle.this.lastWrittenIndex();
        }

        @Override
        public long size() {
            return lastWrittenIndex() + 1;
        }

        @Override
        public Chronicle chronicle() {
            return VanillaChronicle.this;
        }

        @Override
        public void close() {
            if (indexBytes != null) {
                indexBytes.release();
            }
            if (dataBytes != null) {
                dataBytes.release();
            }

            super.close();
        }
    }

    private class VanillaTailer extends AbstractVanillaExcerptCommon implements ExcerptTailer {
        private long index = -1;
        private int lastCycle = Integer.MIN_VALUE;
        private int lastIndexCount = Integer.MIN_VALUE;
        private int lastThreadId = Integer.MIN_VALUE;
        private int lastDataCount = Integer.MIN_VALUE;

        @Override
        public long index() {
            return index;
        }

        public boolean index(long nextIndex) {
            checkNotClosed();
            try {
                int cycle = (int) (nextIndex >>> entriesForCycleBits);
                int indexCount = (int) ((nextIndex & entriesForCycleMask) >>> indexBlockLongsBits);
                int indexOffset = (int) (nextIndex & indexBlockLongsMask);
                long indexValue;
                boolean indexFileChange = false;
                try {
                    if (lastCycle != cycle || lastIndexCount != indexCount || indexBytes == null) {
                        if (indexBytes != null) {
                            indexBytes.release();
                            indexBytes = null;
                        }
                        if (dataBytes != null) {
                            dataBytes.release();
                            dataBytes = null;
                        }

                        indexBytes = indexCache.indexFor(cycle, indexCount, false);
                        indexFileChange = true;
                        assert indexBytes.refCount() > 1;
                        lastCycle = cycle;
                        lastIndexCount = indexCount;
                    }
                    indexValue = indexBytes.readVolatileLong(indexOffset << 3);
                } catch (FileNotFoundException e) {
                    return false;
                }
                if (indexValue == 0) {
                    return false;
                }
                int threadId = (int) (indexValue >>> INDEX_DATA_OFFSET_BITS);
                long dataOffset0 = indexValue & INDEX_DATA_OFFSET_MASK;
                int dataCount = (int) (dataOffset0 >>> dataBlockSizeBits);
                int dataOffset = (int) (dataOffset0 & dataBlockSizeMask);
                if (lastThreadId != threadId || lastDataCount != dataCount || indexFileChange) {
                    if (dataBytes != null) {
                        dataBytes.release();
                        dataBytes = null;
                    }
                }
                if (dataBytes == null) {
                    dataBytes = dataCache.dataFor(cycle, threadId, dataCount, false);
                    lastThreadId = threadId;
                    lastDataCount = dataCount;
                }

                int len = dataBytes.readVolatileInt(dataOffset - 4);
                if (len == 0)
                    return false;

                int len2 = ~len;
                // invalid if either the top two bits are set,
                if ((len2 >>> 30) != 0)
                    throw new IllegalStateException("Corrupted length " + Integer.toHexString(len));
                startAddr = positionAddr = dataBytes.startAddr() + dataOffset;
                limitAddr = startAddr + ~len;
                index = nextIndex;
                finished = false;
                return true;
            } catch (IOException ioe) {
                throw new AssertionError(ioe);
            }
        }

        public boolean nextIndex() {
            checkNotClosed();
            if (index < 0) {
                toStart();
                if (index < 0)
                    return false;
            }

            long nextIndex = index + 1;
            while (true) {
                boolean found = index(nextIndex);
                if (found)
                    return true;

                int cycle = (int) (nextIndex / config.entriesPerCycle());
                if (cycle >= cycle())
                    return false;

                nextIndex = (cycle + 1) * config.entriesPerCycle();
            }
        }

        @NotNull
        public ExcerptTailer toStart() {
            int cycle = (int) indexCache.firstCycle();
            if (cycle >= 0) {
                index = (cycle * config.entriesPerCycle()) - 1;
            }

            return this;
        }

        @NotNull
        @Override
        public ExcerptTailer toEnd() {
            long lastIndex = lastIndex();
            if (lastIndex >= 0) {
                index(lastIndex);
            } else {
                return toStart();
            }

            return this;
        }

        @Override
        public long capacity() {
            return limitAddr - startAddr;
        }
    }

    private class VanillaExcerpt extends VanillaTailer implements Excerpt {
        public long findMatch(@NotNull ExcerptComparator comparator) {
            throw new UnsupportedOperationException();
        }

        public void findRange(@NotNull long[] startEnd, @NotNull ExcerptComparator comparator) {
            throw new UnsupportedOperationException();
        }

        @NotNull
        @Override
        public Excerpt toStart() {
            super.toStart();
            return this;
        }

        @NotNull
        @Override
        public Excerpt toEnd() {
            super.toEnd();
            return this;
        }
    }

    public class VanillaAppender extends AbstractVanillaExcerptCommon implements ExcerptAppender {
        private final long id = appenderCount.incrementAndGet();
        private int lastCycle = Integer.MIN_VALUE;
        private int lastThreadId = Integer.MIN_VALUE;
        private int appenderCycle;
        private int appenderThreadId;
        private boolean nextSynchronous;
        private int dataCount;

        @Override
        public void startExcerpt() {
            startExcerpt(config.defaultMessageSize());
        }

        @Override
        public void startExcerpt(long capacity) {
            startExcerpt(capacity, cycle());
        }

        public void startExcerpt(long capacity, int cycle) {
            checkNotClosed();
            try {
                appenderCycle = cycle;
                appenderThreadId = AffinitySupport.getThreadId();
                assert (appenderThreadId & THREAD_ID_MASK) == appenderThreadId : "appenderThreadId: " + appenderThreadId;

                if (appenderCycle != lastCycle || appenderThreadId != lastThreadId) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("CHANGE Id %03d Cycle %08x LastC %08x Thread %016x LastT %016x", id, appenderCycle, lastCycle, appenderThreadId, lastThreadId));
                    }
                    if (dataBytes != null) {
                        dataBytes.release();
                        dataBytes = null;
                    }
                    if (indexBytes != null) {
                        indexBytes.release();
                        indexBytes = null;
                    }

                    dataCount = dataCache.findNextDataCount(appenderCycle, appenderThreadId);
                    dataBytes = dataCache.dataFor(appenderCycle, appenderThreadId, dataCount, true);
                    lastCycle = appenderCycle;
                    lastThreadId = appenderThreadId;
                }

                if (dataBytes.remaining() < capacity + 4) {
                    dataBytes.release();
                    dataBytes = null;
                    dataCount++;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("ROLL Id %03d Cycle %08x Thread %016x Count %04d", id, appenderCycle, appenderThreadId, dataCount));
                    }
                    dataBytes = dataCache.dataFor(appenderCycle, appenderThreadId, dataCount, true);
                }

                startAddr = positionAddr = dataBytes.positionAddr() + 4;
                limitAddr = startAddr + capacity;
                nextSynchronous = config.synchronous();
                finished = false;
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public void addPaddedEntry() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean nextSynchronous() {
            return nextSynchronous;
        }

        @Override
        public void nextSynchronous(boolean nextSynchronous) {
            this.nextSynchronous = nextSynchronous;
        }

        @Override
        public void finish() {
            super.finish();
            long entryLen = positionAddr - startAddr;
            int length = ~(int) entryLen;
            NativeBytes.UNSAFE.putOrderedInt(null, startAddr - 4, length);
            // position of the start not the end.
            int offset = (int) (startAddr - dataBytes.address());
            long dataOffset = dataBytes.index() * config.dataBlockSize() + offset;
            long indexValue = ((long) appenderThreadId << INDEX_DATA_OFFSET_BITS) + dataOffset;
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("FINISH Id %03d Thread %016x Index %016x Len %016x Start %016x Pos %016x", id, appenderThreadId, indexValue, entryLen, startAddr, positionAddr));
            }
            lastWrittenIndex = indexValue;
            try {
                final boolean appendDone = (indexBytes != null) && VanillaIndexCache.append(indexBytes, indexValue, nextSynchronous);
                if (!appendDone) {
                    if (indexBytes != null) {
                        indexBytes.release();
                        indexBytes = null;
                    }

                    indexBytes = indexCache.append(appenderCycle, indexValue, nextSynchronous);
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }

            dataBytes.positionAddr(positionAddr);
            dataBytes.alignPositionAddr(4);

            if (nextSynchronous) {
                dataBytes.force();
            }
        }

        @Override
        public long index() {
            return -1;
        }

        @NotNull
        @Override
        public ExcerptAppender toEnd() {
            // NO-OP
            return this;
        }
    }

}
