package driver;

import sun.misc.Unsafe;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public final class ClusterBootstrap {

    private static final Unsafe UNSAFE;
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new AssertionError("Failed to initialize sun.misc.Unsafe", e);
        }
    }

    public static final class DirectAnnouncementRing {
        private final long baseAddress;
        private final int capacityBytes;
        private final AtomicLong writeIndex = new AtomicLong(0);
        private final AtomicLong readIndex = new AtomicLong(0);

        public DirectAnnouncementRing(int capacityBytes) {
            this.capacityBytes = capacityBytes;
            this.baseAddress = UNSAFE.allocateMemory(capacityBytes);
            UNSAFE.setMemory(baseAddress, capacityBytes, (byte) 0);
        }

        public boolean offer(byte[] data, int length) {
            if (length + 4 > capacityBytes) {
                return false;
            }
            long currentWrite = writeIndex.get();
            long currentRead = readIndex.get();
            
            if (currentWrite - currentRead + length + 4 > capacityBytes) {
                return false;
            }

            long targetOffset = baseAddress + (currentWrite % capacityBytes);
            if ((currentWrite % capacityBytes) + length + 4 <= capacityBytes) {
                UNSAFE.putInt(targetOffset, length);
                UNSAFE.copyMemory(data, 0, null, targetOffset + 4, length);
            } else {
                int firstPart = (int) (capacityBytes - (currentWrite % capacityBytes) - 4);
                UNSAFE.putInt(targetOffset, length);
                if (firstPart > 0) {
                    UNSAFE.copyMemory(data, 0, null, targetOffset + 4, firstPart);
                }
                UNSAFE.copyMemory(data, firstPart, null, baseAddress, length - firstPart);
            }

            writeIndex.addAndGet(length + 4);
            return true;
        }

        public int poll(byte[] destination, int maxDestinationLength) {
            long currentRead = readIndex.get();
            long currentWrite = writeIndex.get();

            if (currentRead >= currentWrite) {
                return 0;
            }

            long targetOffset = baseAddress + (currentRead % capacityBytes);
            int length;
            if ((currentRead % capacityBytes) + 4 <= capacityBytes) {
                length = UNSAFE.getInt(targetOffset);
            } else {
                int p1 = (int) (capacityBytes - (currentRead % capacityBytes));
                byte[] lenBuf = new byte[4];
                if (p1 > 0) {
                    UNSAFE.copyMemory(null, targetOffset, lenBuf, 0, p1);
                    UNSAFE.copyMemory(null, baseAddress, lenBuf, p1, 4 - p1);
                } else {
                    UNSAFE.copyMemory(null, baseAddress, lenBuf, 0, 4);
                }
                length = java.nio.ByteBuffer.wrap(lenBuf).getInt();
            }

            if (length > maxDestinationLength) {
                return -1;
            }

            long dataOffset = currentRead + 4;
            if ((dataOffset % capacityBytes) + length <= capacityBytes) {
                UNSAFE.copyMemory(null, baseAddress + (dataOffset % capacityBytes), destination, 0, length);
            } else {
                int firstPart = (int) (capacityBytes - (dataOffset % capacityBytes));
                if (firstPart > 0) {
                    UNSAFE.copyMemory(null, baseAddress + (dataOffset % capacityBytes), destination, 0, firstPart);
                }
                UNSAFE.copyMemory(null, baseAddress, destination, firstPart, length - firstPart);
            }

            readIndex.addAndGet(length + 4);
            return length;
        }

        public void release() {
            UNSAFE.freeMemory(baseAddress);
        }
    }

    public static final class VectorClock implements Comparable<VectorClock> {
        private final long[] nodeHashes;
        private final long[] counters;

        public VectorClock(long[] nodeHashes, long[] counters) {
            this.nodeHashes = nodeHashes;
            this.counters = counters;
        }

        public VectorClock increment(long nodeHash) {
            int idx = Arrays.binarySearch(nodeHashes, nodeHash);
            if (idx >= 0) {
                long[] newCounters = counters.clone();
                newCounters[idx]++;
                return new VectorClock(nodeHashes, newCounters);
            } else {
                int insertPos = -idx - 1;
                long[] newHashes = new long[nodeHashes.length + 1];
                long[] newCounters = new long[counters.length + 1];
                System.arraycopy(nodeHashes, 0, newHashes, 0, insertPos);
                System.arraycopy(counters, 0, newCounters, 0, insertPos);
                newHashes[insertPos] = nodeHash;
                newCounters[insertPos] = 1L;
                System.arraycopy(nodeHashes, insertPos, newHashes, insertPos + 1, nodeHashes.length - insertPos);
                System.arraycopy(counters, insertPos, newCounters, insertPos + 1, counters.length - insertPos);
                return new VectorClock(newHashes, newCounters);
            }
        }

        @Override
        public int compareTo(VectorClock o) {
            boolean thisGreaterOrEqual = true;
            boolean otherGreaterOrEqual = true;

            int i = 0, j = 0;
            while (i < this.nodeHashes.length || j < o.nodeHashes.length) {
                long h1 = (i < this.nodeHashes.length) ? this.nodeHashes[i] : Long.MAX_VALUE;
                long h2 = (j < o.nodeHashes.length) ? o.nodeHashes[j] : Long.MAX_VALUE;

                long c1 = (i < this.nodeHashes.length && h1 == h1) ? this.counters[i] : 0L;
                long c2 = (j < o.nodeHashes.length && h2 == h2) ? o.counters[j] : 0L;

                if (h1 == h2) {
                    if (c1 < c2) thisGreaterOrEqual = false;
                    if (c1 > c2) otherGreaterOrEqual = false;
                    i++;
                    j++;
                } else if (h1 < h2) {
                    if (c1 > 0) otherGreaterOrEqual = false;
                    i++;
                } else {
                    if (c2 > 0) thisGreaterOrEqual = false;
                    j++;
                }
            }

            if (thisGreaterOrEqual && otherGreaterOrEqual) return 0;
            if (thisGreaterOrEqual) return 1;
            if (otherGreaterOrEqual) return -1;
            return 0; // Concurrent / Conflict
        }
    }

    public enum NodeState {
        ALIVE, SUSPECTED, DEAD
    }

    public static final class MembershipRecord {
        public final long nodeHash;
        public final NodeState state;
        public final long generation;
        public final long suspicionEpoch;
        public final VectorClock vectorClock;

        public MembershipRecord(long nodeHash, NodeState state, long generation, long suspicionEpoch, VectorClock vectorClock) {
            this.nodeHash = nodeHash;
            this.state = state;
            this.generation = generation;
            this.suspicionEpoch = suspicionEpoch;
            this.vectorClock = vectorClock;
        }
    }

    public static final class SpinLatch {
        private final AtomicInteger state = new AtomicInteger(0);

        public void acquire() {
            int spins = 0;
            while (!state.compareAndSet(0, 1)) {
                if (spins < 64) {
                    Thread.onSpinWait();
                    spins++;
                } else if (spins < 1024) {
                    Thread.yield();
                    spins++;
                } else {
                    LockSupport.parkNanos(1000L);
                }
            }
        }

        public void release() {
            state.set(0);
            LockSupport.unpark(Thread.currentThread());
        }
    }

    public static final class ClusterEngine {
        private final long localNodeHash;
        private final DirectAnnouncementRing announcementRing;
        private final SpinLatch latch;
        private volatile MembershipRecord[] membershipTable;
        private final AtomicLong generationCounter;
        private final DatagramChannel channel;
        private volatile boolean running;

        public ClusterEngine(long localNodeHash, int ringBufferSize, InetSocketAddress bindAddress) throws Exception {
            this.localNodeHash = localNodeHash;
            this.announcementRing = new DirectAnnouncementRing(ringBufferSize);
            this.latch = new SpinLatch();
            this.generationCounter = new AtomicLong(1L);
            this.membershipTable = new MembershipRecord[0];
            this.channel = DatagramChannel.open();
            this.channel.configureBlocking(false);
            this.channel.bind(bindAddress);
            this.running = true;
        }

        public void updateState(long targetNodeHash, NodeState newState, long suspicionEpoch) {
            latch.acquire();
            try {
                int index = -1;
                for (int i = 0; i < membershipTable.length; i++) {
                    if (membershipTable[i].nodeHash == targetNodeHash) {
                        index = i;
                        break;
                    }
                }

                long gen = generationCounter.get();
                VectorClock vc = (index >= 0) ? membershipTable[index].vectorClock.increment(localNodeHash) : new VectorClock(new long[]{localNodeHash}, new long[]{1L});
                MembershipRecord record = new MembershipRecord(targetNodeHash, newState, gen, suspicionEpoch, vc);

                MembershipRecord[] newTable;
                if (index >= 0) {
                    newTable = membershipTable.clone();
                    newTable[index] = record;
                } else {
                    newTable = Arrays.copyOf(membershipTable, membershipTable.length + 1);
                    newTable[newTable.length - 1] = record;
                    Arrays.sort(newTable, (a, b) -> Long.compare(a.nodeHash, b.nodeHash));
                }
                membershipTable = newTable;

                byte[] serialized = serializeRecord(record);
                announcementRing.offer(serialized, serialized.length);
            } finally {
                latch.release();
            }
        }

        private byte[] serializeRecord(MembershipRecord record) {
            ByteBuffer buf = ByteBuffer.allocate(36);
            buf.putLong(record.nodeHash);
            buf.putInt(record.state.ordinal());
            buf.putLong(record.generation);
            buf.putLong(record.suspicionEpoch);
            buf.putLong(record.vectorClock.nodeHashes.length); // simplified serialization payload
            return buf.array();
        }

        public void pollAndTransmit(InetSocketAddress destination) {
            byte[] gossipBuf = new byte[1024];
            int readBytes = announcementRing.poll(gossipBuf, gossipBuf.length);
            if (readBytes > 0) {
                try {
                    ByteBuffer outBuf = ByteBuffer.wrap(gossipBuf, 0, readBytes);
                    channel.send(outBuf, destination);
                } catch (Exception e) {
                    // Suppress network IO exceptions in tight loop for resilience
                }
            }
        }

        public void shutdown() {
            this.running = false;
            try {
                channel.close();
            } catch (Exception ignored) {}
            announcementRing.release();
        }
    }

    public static void main(String[] args) throws Exception {
        long nodeHash = System.currentTimeMillis();
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 8080);
        ClusterEngine engine = new ClusterEngine(nodeHash, 65536, address);
        
        engine.updateState(nodeHash, NodeState.ALIVE, 0L);
        
        Thread worker = new Thread(() -> {
            while (true) {
                engine.pollAndTransmit(new InetSocketAddress("127.0.0.1", 8081));
                LockSupport.parkNanos(1000000L); // 1ms sleep equivalent via parking
            }
        });
        worker.setDaemon(true);
        worker.start();

        Thread.sleep(100);
        engine.shutdown();
    }
}