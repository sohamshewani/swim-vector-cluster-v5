package swim.vector.cluster;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exhaustive unit, invariant, and concurrent stress-test suite for 'swim-vector-cluster'.
 * Implements a high-fidelity model of SWIM (Scalable Weakly-consistent Infection-style Process Group)
 * protocol integrated with Vector Clocks for causal ordering and conflict resolution.
 */
public class SwimVectorClusterTest {

    // ===================================================================================
    // PRODUCTION-GRADE MODEL IMPLEMENTATION OF SWIM + VECTOR CLUSTER
    // ===================================================================================

    public enum NodeState {
        ALIVE, SUSPECT, DEAD, LEFT
    }

    public static class VectorClock implements Comparable<VectorClock>, Cloneable {
        private final ConcurrentHashMap<String, Long> clock;

        public VectorClock() {
            this.clock = new ConcurrentHashMap<>();
        }

        public VectorClock(Map<String, Long> initial) {
            this.clock = new ConcurrentHashMap<>(initial);
        }

        public void increment(String nodeId) {
            clock.compute(nodeId, (k, v) -> v == null ? 1L : v + 1L);
        }

        public long get(String nodeId) {
            return clock.getOrDefault(nodeId, 0L);
        }

        public void merge(VectorClock other) {
            for (Map.Entry<String, Long> entry : other.clock.entrySet()) {
                clock.merge(entry.getKey(), entry.getValue(), Math::max);
            }
        }

        @Override
        public VectorClock clone() {
            return new VectorClock(this.clock);
        }

        /**
         * Compares this vector clock with another.
         * Returns -1 if this < other (happens-before),
         *          1 if this > other (happens-after),
         *          0 if concurrent or identical.
         */
        @Override
        public int compareTo(VectorClock o) {
            boolean thisLess = false;
            boolean thisGreater = false;

            Set<String> allKeys = new HashSet<>(this.clock.keySet());
            allKeys.addAll(o.clock.keySet());

            for (String key : allKeys) {
                long v1 = this.get(key);
                long v2 = o.get(key);
                if (v1 < v2) {
                    thisLess = true;
                } else if (v1 > v2) {
                    thisGreater = true;
                }
            }

            if (thisLess && thisGreater) return 0; // Concurrent
            if (thisLess) return -1;
            if (thisGreater) return 1;
            return 0; // Identical
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof VectorClock)) return false;
            VectorClock that = (VectorClock) o;
            Set<String> allKeys = new HashSet<>(this.clock.keySet());
            allKeys.addAll(that.clock.keySet());
            for (String k : allKeys) {
                if (this.get(k) != that.get(k)) return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            return clock.hashCode();
        }

        @Override
        public String toString() {
            return new ArrayList<>(clock.entrySet()).stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(e -> e.getKey() + ":" + e.getValue())
                    .collect(Collectors.joining(",", "{", "}"));
        }
    }

    public static class MemberMetadata {
        private final String id;
        private volatile NodeState state;
        private volatile long incarnation;
        private final VectorClock vectorClock;
        private volatile long lastUpdatedTimestamp;

        public MemberMetadata(String id, NodeState state, long incarnation, VectorClock vectorClock) {
            this.id = id;
            this.state = state;
            this.incarnation = incarnation;
            this.vectorClock = vectorClock != null ? vectorClock : new VectorClock();
            this.lastUpdatedTimestamp = System.currentTimeMillis();
        }

        public String getId() { return id; }
        public NodeState getState() { return state; }
        public void setState(NodeState state) { 
            this.state = state; 
            this.lastUpdatedTimestamp = System.currentTimeMillis();
        }
        public long getIncarnation() { return incarnation; }
        public void setIncarnation(long incarnation) { 
            this.incarnation = incarnation; 
            this.lastUpdatedTimestamp = System.currentTimeMillis();
        }
        public VectorClock getVectorClock() { return vectorClock; }
        public long getLastUpdatedTimestamp() { return lastUpdatedTimestamp; }
    }

    public static class MembershipMessage {
        private final String senderId;
        private final String targetId;
        private final NodeState state;
        private final long incarnation;
        private final VectorClock vectorClock;

        public MembershipMessage(String senderId, String targetId, NodeState state, long incarnation, VectorClock vectorClock) {
            this.senderId = senderId;
            this.targetId = targetId;
            this.state = state;
            this.incarnation = incarnation;
            this.vectorClock = vectorClock;
        }

        public String getSenderId() { return senderId; }
        public String getTargetId() { return targetId; }
        public NodeState getState() { return state; }
        public long getIncarnation() { return incarnation; }
        public VectorClock getVectorClock() { return vectorClock; }
    }

    public interface NetworkLayer {
        void send(String destinationId, MembershipMessage message);
        List<MembershipMessage> pollInbox(String nodeId);
    }

    public static class SimulatedClusterNetwork implements NetworkLayer {
        private final ConcurrentHashMap<String, ConcurrentLinkedQueue<MembershipMessage>> inboxes = new ConcurrentHashMap<>();
        private volatile double dropRate = 0.0;
        private volatile long networkLatencyMs = 0;
        private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

        public void registerNode(String nodeId) {
            inboxes.putIfAbsent(nodeId, new ConcurrentLinkedQueue<>());
        }

        public void unregisterNode(String nodeId) {
            inboxes.remove(nodeId);
        }

        public void setDropRate(double dropRate) {
            this.dropRate = dropRate;
        }

        public void setNetworkLatencyMs(long latency) {
            this.networkLatencyMs = latency;
        }

        @Override
        public void send(String destinationId, MembershipMessage message) {
            if (Math.random() < dropRate) {
                return; // Simulate packet loss
            }
            ConcurrentLinkedQueue<MembershipMessage> inbox = inboxes.get(destinationId);
            if (inbox != null) {
                if (networkLatencyMs > 0) {
                    scheduler.schedule(() -> inbox.add(message), networkLatencyMs, TimeUnit.MILLISECONDS);
                } else {
                    inbox.add(message);
                }
            }
        }

        @Override
        public List<MembershipMessage> pollInbox(String nodeId) {
            ConcurrentLinkedQueue<MembershipMessage> inbox = inboxes.get(nodeId);
            if (inbox == null) return Collections.emptyList();
            List<MembershipMessage> msgs = new ArrayList<>();
            MembershipMessage msg;
            while ((msg = inbox.poll()) != null) {
                msgs.add(msg);
            }
            return msgs;
        }

        public void shutdown() {
            scheduler.shutdownNow();
        }
    }

    public static class SwimVectorNode {
        private final String nodeId;
        private final SimulatedClusterNetwork network;
        private final ConcurrentHashMap<String, MemberMetadata> members = new ConcurrentHashMap<>();
        private final VectorClock localVectorClock = new VectorClock();
        private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
        private volatile long localIncarnation = 0;
        private volatile boolean running = false;
        private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        public SwimVectorNode(String nodeId, SimulatedClusterNetwork network) {
            this.nodeId = nodeId;
            this.network = network;
            // Self initialization
            VectorClock initialVc = new VectorClock();
            initialVc.increment(nodeId);
            members.put(nodeId, new MemberMetadata(nodeId, NodeState.ALIVE, 0, initialVc));
            localVectorClock.merge(initialVc);
            network.registerNode(nodeId);
        }

        public void start() {
            running = true;
            executor.scheduleAtFixedRate(this::backgroundLoop, 50, 50, TimeUnit.MILLISECONDS);
        }

        public void stop() {
            running = false;
            executor.shutdownNow();
            network.unregisterNode(nodeId);
        }

        private void backgroundLoop() {
            if (!running) return;
            try {
                // 1. Process incoming network messages
                List<MembershipMessage> messages = network.pollInbox(nodeId);
                for (MembershipMessage msg : messages) {
                    applyMessage(msg);
                }

                // 2. Periodic gossip / state synchronization
                gossipRandomMember();
            } catch (Exception e) {
                // Ignore thread interruptions or transient errors
            }
        }

        public void applyMessage(MembershipMessage msg) {
            rwLock.writeLock().lock();
            try {
                // Merge vector clocks
                localVectorClock.merge(msg.getVectorClock());
                localVectorClock.increment(nodeId);

                members.compute(msg.getTargetId(), (id, existing) -> {
                    if (existing == null) {
                        // New node discovered
                        VectorClock newVc = msg.getVectorClock().clone();
                        return new MemberMetadata(id, msg.getState(), msg.getIncarnation(), newVc);
                    } else {
                        // Conflict resolution based on SWIM incarnation rules + Vector Clocks
                        if (msg.getIncarnation() > existing.getIncarnation()) {
                            existing.setIncarnation(msg.getIncarnation());
                            existing.setState(msg.getState());
                            existing.getVectorClock().merge(msg.getVectorClock());
                        } else if (msg.getIncarnation() == existing.getIncarnation()) {
                            // Same incarnation: resolve via Vector Clock precedence
                            int vcCmp = msg.getVectorClock().compareTo(existing.getVectorClock());
                            if (vcCmp > 0) {
                                existing.setState(msg.getState());
                                existing.getVectorClock().merge(msg.getVectorClock());
                            } else if (vcCmp == 0) {
                                // Idempotent or state escalation precedence: DEAD > SUSPECT > ALIVE
                                if (precedence(msg.getState()) > precedence(existing.getState())) {
                                    existing.setState(msg.getState());
                                }
                            }
                            // if vcCmp < 0, ignore stale message
                        }
                        // if incoming incarnation is lower, ignore stale message
                        return existing;
                    }
                });
            } finally {
                rwLock.writeLock().unlock();
            }
        }

        private int precedence(NodeState state) {
            switch (state) {
                case DEAD: return 3;
                case SUSPECT: return 2;
                case LEFT: return 1;