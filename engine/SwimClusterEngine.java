package engine;

import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;

public final class SwimClusterEngine {

    public static final Unsafe UNSAFE;
    static {
        Unsafe unsafe;
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            unsafe = (Unsafe) f.get(null);
        } catch (Exception e) {
            try {
                Field f = Unsafe.class.getDeclaredField("THE_UNSAFE");
                f.setAccessible(true);
                unsafe = (Unsafe) f.get(null);
            } catch (Exception ex) {
                throw new RuntimeException("Unable to acquire sun.misc.Unsafe", ex);
            }
        }
        UNSAFE = unsafe;
    }

    public enum NodeState {
        ALIVE(0),
        SUSPECTED(1),
        DEAD(2);

        private final int code;
        NodeState(int code) {
            this.code = code;
        }
        public int code() {
            return code;
        }
        public static NodeState fromCode(int code) {
            switch (code) {
                case 0: return ALIVE;
                case 1: return SUSPECTED;
                case 2: return DEAD;
                default: throw new IllegalArgumentException("Unknown state code: " + code);
            }
        }
    }

    public static final class DirectAnnouncementRing {
        private final long address;
        private final int capacity;
        private final int entrySize;
        private static final long HEAD_OFFSET;
        private static final long TAIL_OFFSET;

        private volatile long head = 0L;
        private volatile long tail = 0L;

        static {
            try {
                HEAD_OFFSET = UNSAFE.objectFieldOffset(DirectAnnouncementRing.class.getDeclaredField("head"));
                TAIL_OFFSET = UNSAFE.objectFieldOffset(DirectAnnouncementRing.class.getDeclaredField("tail"));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public DirectAnnouncementRing(int capacity, int entrySize) {
            if ((capacity & (capacity - 1)) != 0) {
                throw new IllegalArgumentException("Capacity must be a power of two");
            }
            this.capacity = capacity;
            this.entrySize = entrySize;
            long totalBytes = (long) capacity * entrySize;
            this.address = UNSAFE.allocateMemory(totalBytes);
            UNSAFE.setMemory(this.address, totalBytes, (byte) 0);
        }

        public boolean offer(long srcNodeHash, int stateCode, long generation, long sequence) {
            long currentTail = tail;
            long currentHead = head;
            if (currentTail - currentHead >= capacity) {
                return false;
            }
            if (!UNSAFE.compareAndSwapLong(this, TAIL_OFFSET, currentTail, currentTail + 1)) {
                return false;
            }

            long index = currentTail & (capacity - 1);
            long entryAddress = address + (index * entrySize);

            UNSAFE.putLong(entryAddress, srcNodeHash);
            UNSAFE.putInt(entryAddress + 8, stateCode);
            UNSAFE.putLong(entryAddress + 12, generation);
            UNSAFE.putLong(entryAddress + 20, sequence);
            return true;
        }

        public int drainTo(ByteBuffer dst, int maxEntries) {
            int drained = 0;
            while (drained < maxEntries) {
                long currentHead = head;
                long currentTail = tail;
                if (currentHead >= currentTail) {
                    break;
                }
                long index = currentHead & (capacity - 1);
                long entryAddress = address + (index * entrySize);

                long srcNodeHash = UNSAFE.getLong(entryAddress);
                int stateCode = UNSAFE.getInt(entryAddress + 8);
                long generation = UNSAFE.getLong(entryAddress + 12);
                long sequence = UNSAFE.getLong(entryAddress + 20);

                if (dst.remaining() < entrySize) {
                    break;
                }

                if (!UNSAFE.compareAndSwapLong(this, HEAD_OFFSET, currentHead, currentHead + 1)) {
                    continue;
                }

                dst.putLong(srcNodeHash);
                dst.putInt(stateCode);
                dst.putLong(generation);
                dst.putLong(sequence);
                drained++;
            }
            return drained;
        }

        public void close() {
            UNSAFE.freeMemory(address);
        }
    }

    public static final class VectorClock implements Comparable<VectorClock> {
        private final long[] nodeHashes;
        private final long[] sequences;

        public VectorClock(long[] nodeHashes, long[] sequences) {
            if (nodeHashes.length != sequences.length) {
                throw new IllegalArgumentException("Mismatched arrays");
            }
            this.nodeHashes = nodeHashes;
            this.sequences = sequences;
        }

        public static VectorClock empty() {
            return new VectorClock(new long[0], new long[0]);
        }

        public VectorClock increment(long selfHash) {
            int idx = Arrays.binarySearch(nodeHashes, selfHash);
            if (idx >= 0) {
                long[] newSeqs = sequences.clone();
                newSeqs[idx]++;
                return new VectorClock(nodeHashes, newSeqs);
            } else {
                int insertionPoint = -idx - 1;
                long[] newHashes = new long[nodeHashes.length + 1];
                long[] newSeqs = new long[sequences.length + 1];

                System.arraycopy(nodeHashes, 0, newHashes, 0, insertionPoint);
                newHashes[insertionPoint] = selfHash;
                System.arraycopy(nodeHashes, insertionPoint, newHashes, insertionPoint + 1, nodeHashes.length - insertionPoint);

                System.arraycopy(sequences, 0, newSeqs, 0, insertionPoint);
                newSeqs[insertionPoint] = 1L;
                System.arraycopy(sequences, insertionPoint, newSeqs, insertionPoint + 1, sequences.length - insertionPoint);

                return new VectorClock(newHashes, newSeqs);
            }
        }

        public VectorClock merge(VectorClock other) {
            int i = 0, j = 0;
            int estSize = this.nodeHashes.length + other.nodeHashes.length;
            long[] tempHashes = new long[estSize];
            long[] tempSeqs = new long[estSize];
            int k = 0;

            while (i < this.nodeHashes.length && j < other.nodeHashes.length) {
                if (this.nodeHashes[i] < other.nodeHashes[j]) {
                    tempHashes[k] = this.nodeHashes[i];
                    tempSeqs[k] = this.sequences[i];
                    i++;
                } else if (this.nodeHashes[i] > other.nodeHashes[j]) {
                    tempHashes[k] = other.nodeHashes[j];
                    tempSeqs[k] = other.sequences[j];
                    j++;
                } else {
                    tempHashes[k] = this.nodeHashes[i];
                    tempSeqs[k] = Math.max(this.sequences[i], other.sequences[j]);
                    i++;
                    j++;
                }
                k++;
            }

            while (i < this.nodeHashes.length) {
                tempHashes[k] = this.nodeHashes[i];
                tempSeqs[k] = this.sequences[i];
                i++;
                k++;
            }

            while (j < other.nodeHashes.length) {
                tempHashes[k] = other.nodeHashes[j];
                tempSeqs[k] = other.sequences[j];
                j++;
                k++;
            }

            return new VectorClock(Arrays.copyOf(tempHashes, k), Arrays.copyOf(tempSeqs, k));
        }

        @Override
        public int compareTo(VectorClock o) {
            boolean thisLess = false;
            boolean otherLess = false;

            int i = 0, j = 0;
            while (i < this.nodeHashes.length && j < o.nodeHashes.length) {
                if (this.nodeHashes[i] < o.nodeHashes[j]) {
                    if (this.sequences[i] > 0) otherLess = true;
                    i++;
                } else if (this.nodeHashes[i] > o.nodeHashes[j]) {
                    if (o.sequences[j] > 0) thisLess = true;
                    j++;
                } else {
                    long seq1 = this.sequences[i];
                    long seq2 = o.sequences[j];
                    if (seq1 < seq2) thisLess = true;
                    if (seq1 > seq2) otherLess = true;
                    i++;
                    j++;
                }
            }

            while (i < this.nodeHashes.length) {
                if (this.sequences[i] > 0) otherLess = true;
                i++;
            }

            while (j < o.nodeHashes.length) {
                if (o.sequences[j] > 0) thisLess = true;
                j++;
            }

            if (thisLess && !otherLess) return -1;
            if (otherLess && !thisLess) return 1;
            if (!thisLess && !otherLess) return 0;
            return 2; // Concurrent
        }

        public long getSequence(long nodeHash) {
            int idx = Arrays.binarySearch(nodeHashes, nodeHash);
            return idx >= 0 ? sequences[idx] : 0L;
        }

        public byte[] serialize() {
            ByteBuffer buf = ByteBuffer.allocate(4 + nodeHashes.length * 16);
            buf.putInt(nodeHashes.length);
            for (int i = 0; i < nodeHashes.length; i++) {
                buf.putLong(nodeHashes[i]);
                buf.putLong(sequences[i]);
            }
            return buf.array();
        }

        public static VectorClock deserialize(ByteBuffer buf) {
            int len = buf.getInt();
            long[] hashes = new long[len];
            long[] seqs = new long[len];
            for (int i = 0; i < len; i++) {
                hashes[i] = buf.getLong();
                seqs[i] = buf.getLong();
            }
            return new VectorClock(hashes, seqs);
        }
    }

    public static final class NodeStateRecord {
        private final long nodeHash;
        private final InetSocketAddress address;
        private volatile NodeState state;
        private volatile long generation;
        private volatile long suspicionTimestamp;
        private volatile VectorClock vectorClock;

        private static final AtomicReferenceFieldUpdater<NodeStateRecord, NodeState> STATE_UPDATER =
                AtomicReferenceFieldUpdater.newUpdater(NodeStateRecord.class, NodeState.class, "state");

        private static final AtomicLongFieldUpdater<NodeStateRecord, Long> GEN_UPDATER =
                AtomicLongFieldUpdater.newUpdater(NodeStateRecord.class, "generation");

        public NodeStateRecord(long nodeHash, InetSocketAddress address, NodeState state, long generation, VectorClock vectorClock) {
            this.nodeHash = nodeHash;
            this.address = address;
            this.state = state;
            this.generation = generation;
            this.suspicionTimestamp = 0L;
            this.vectorClock = vectorClock;
        }

        public long getNodeHash() { return nodeHash; }
        public InetSocketAddress getAddress() { return address; }
        public NodeState getState() { return state; }
        public long getGeneration() { return generation; }
        public long getSuspicionTimestamp() { return suspicionTimestamp; }
        public VectorClock getVectorClock() { return vectorClock; }

        public boolean compareAndSetState(NodeState expected, NodeState update) {
            return STATE_UPDATER.compareAndSet(this, expected, update);
        }

        public void setState(NodeState state) {
            this.state = state;
        }

        public boolean updateGenerationAndClock(long newGen, VectorClock newClock) {
            long curGen = generation;
            if (newGen > curGen) {
                if (GEN_UPDATER.compareAndSet(this, curGen, newGen)) {
                    this.vectorClock = newClock;
                    return true;
                }
            } else if (newGen == curGen) {
                VectorClock merged = this.vectorClock.merge(newClock);
                this.vectorClock = merged;
                return true;
            }
            return false;
        }

        public void setSuspicionTimestamp(long ts) {
            this.suspicionTimestamp = ts;
        }
    }

    public static final class SpinLatch {
        private static final int LOCKED = 1;
        private static final int UNLOCKED = 0;
        private volatile int state = UNLOCKED;
        private static final AtomicIntegerFieldUpdater<SpinLatch> STATE_UPDATER =
                AtomicIntegerFieldUpdater.newUpdater(SpinLatch.class, "state");

        public void lock() {
            int spins = 0;
            while (!STATE_UPDATER.compareAndSet(this, UNLOCKED, LOCKED)) {
                if (spins < 100) {
                    Thread.onSpinWait();
                    spins++;
                } else if (spins < 200) {
                    Thread.yield();
                    spins++;
                } else {
                    LockSupport.parkNanos(1000L);
                    spins = 0;
                }
            }
        }

        public void unlock() {
            STATE_UPDATER.set(this, UNLOCKED);
            LockSupport.unpark(Thread.currentThread());
        }
    }

    public static final class EventLoopGroup implements Runnable {
        private final Selector selector;
        private final DatagramChannel channel;
        private final SwimClusterEngine engine;
        private volatile boolean running = true;

        public EventLoopGroup(InetSocketAddress bindAddress, SwimClusterEngine engine) throws IOException {
            this.engine = engine;
            this.selector = Selector.open();
            this.channel = DatagramChannel.open();
            this.channel.configureBlocking(false);
            this.channel.socket().bind(bindAddress);
            this.channel.register(selector, SelectionKey.OP_READ);
        }

        @Override
        public void run() {
            ByteBuffer rxBuffer = ByteBuffer.allocateDirect(65535);
            while (running) {
                try {
                    if (selector.select(10) > 0) {
                        Iterator<SelectionKey> it = selector.selectedKeys().iterator();
                        while (it.hasNext()) {
                            SelectionKey key = it.next();
                            it.remove();
                            if (key.isReadable()) {
                                rxBuffer.clear();
                                InetSocketAddress sender = (InetSocketAddress) channel.receive(rxBuffer);
                                if (sender != null) {
                                    rxBuffer.flip();
                                    engine.handleIncomingPacket(rxBuffer, sender);
                                }
                            }
                        }
                    }
                    engine.tick();
                } catch (IOException e) {
                    if (!running) break;
                }
            }
        }

        public void send(ByteBuffer buffer, InetSocketAddress target) throws IOException {
            channel.send(buffer, target);
        }

        public void shutdown() {
            running = false;
            try {
                selector.wakeup();
                selector.close();
                channel.close();
            } catch (IOException ignored) {}
        }
    }

    private final long selfNodeHash;
    private final InetSocketAddress selfAddress;
    private final ConcurrentMap<Long, NodeStateRecord> membershipMap;
    private final DirectAnnouncementRing announcementRing