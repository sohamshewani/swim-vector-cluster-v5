package com.example.swim.microbenchmark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Dedicated Throughput and Latency Microbenchmark for 'swim-vector-cluster'.
 * Simulates high-concurrency vector routing, membership state updates, and gossip replication.
 */
public class SwimVectorClusterMicrobenchmark {

    // Configuration Constants
    private static final int WARMUP_ITERATIONS = 50_000;
    private static final int MEASUREMENT_ITERATIONS = 200_000;
    private static final int CONCURRENCY_THREADS = Runtime.getRuntime().availableProcessors();
    private static final int VECTOR_DIMENSION = 128;
    private static final int CLUSTER_SIZE = 64;

    public static void main(String[] args) throws InterruptedException {
        System.out.println("Initializing 'swim-vector-cluster' Microbenchmark...");
        System.out.println("Threads: " + CONCURRENCY_THREADS);
        System.out.println("Vector Dimension: " + VECTOR_DIMENSION);
        System.out.println("Cluster Size: " + CLUSTER_SIZE);
        System.out.println("------------------------------------------------------------------");

        // Initialize mock cluster and vector routing engine
        SwimClusterSimulator cluster = new SwimClusterSimulator(CLUSTER_SIZE, VECTOR_DIMENSION);

        // --- WARMUP PHASE ---
        runPhase("Warmup", WARMUP_ITERATIONS, cluster, false);

        // --- MEASUREMENT PHASE ---
        BenchmarkResult result = runPhase("Measurement", MEASUREMENT_ITERATIONS, cluster, true);

        // --- REPORTING ---
        printResults(result);
    }

    private static BenchmarkResult runPhase(String phaseName, int iterations, SwimClusterSimulator cluster, boolean recordLatencies) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENCY_THREADS);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(CONCURRENCY_THREADS);

        int opsPerThread = iterations / CONCURRENCY_THREADS;
        AtomicLong successCount = new AtomicLong(0);
        
        // Use thread-local or synchronized collection for latencies to avoid massive contention
        List<long[]> threadLatencies = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < CONCURRENCY_THREADS; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    long[] latencies = recordLatencies ? new long[opsPerThread] : null;
                    int recordedCount = 0;
                    
                    ThreadLocalRandomAdapter random = new ThreadLocalRandomAdapter();

                    for (int j = 0; j < opsPerThread; j++) {
                        float[] vector = random.nextVector(VECTOR_DIMENSION);
                        String nodeId = "node-" + random.nextInt(CLUSTER_SIZE);

                        long startTime = System.nanoTime();
                        boolean success = cluster.routeAndSyncVector(nodeId, vector);
                        long duration = System.nanoTime() - startTime;

                        if (success) {
                            successCount.incrementAndGet();
                            if (recordLatencies) {
                                latencies[recordedCount++] = duration;
                            }
                        }
                    }

                    if (recordLatencies) {
                        // Trim array if needed, though opsPerThread is fixed
                        long[] trimmed = new long[recordedCount];
                        System.arraycopy(latencies, 0, trimmed, 0, recordedCount);
                        threadLatencies.add(trimmed);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        long startTime = System.nanoTime();
        startLatch.countDown();
        endLatch.await();
        long totalDurationNs = System.nanoTime() - startTime;

        executor.shutdown();

        double durationSec = totalDurationNs / 1_000_000_000.0;
        double opsSec = successCount.get() / durationSec;

        long[] aggregatedLatencies = new long[0];
        if (recordLatencies) {
            int totalSize = threadLatencies.stream().mapToInt(arr -> arr.length).sum();
            aggregatedLatencies = new long[totalSize];
            int destPos = 0;
            for (long[] arr : threadLatencies) {
                System.arraycopy(arr, 0, aggregatedLatencies, destPos, arr.length);
                destPos += arr.length;
            }
            java.util.Arrays.sort(aggregatedLatencies);
        }

        return new BenchmarkResult(phaseName, successCount.get(), durationSec, opsSec, aggregatedLatencies);
    }

    private static void printResults(BenchmarkResult res) {
        long p50 = 0;
        long p99 = 0;
        long p999 = 0;

        if (res.latencies.length > 0) {
            p50 = res.latencies[(int) (res.latencies.length * 0.50)];
            p99 = res.latencies[(int) (res.latencies.length * 0.99)];
            p999 = res.latencies[(int) (res.latencies.length * 0.999)];
        }

        System.out.println("\n==================================================================");
        System.out.println("                  SWIM VECTOR CLUSTER BENCHMARK                   ");
        System.out.println("==================================================================");
        System.out.printf("| %-12s | %-12s | %-12s | %-10s | %-10s |%n", 
                "Metric", "Throughput", "P50 Latency", "P99 Latency", "P99.9 Latency");
        System.out.println("------------------------------------------------------------------");
        System.out.printf("| %-12s | %,10.2f o/s | %10.2f ms | %10.2f ms | %10.2f ms |%n", 
                res.phaseName, 
                res.opsSec, 
                p50 / 1_000_000.0, 
                p99 / 1_000_000.0, 
                p999 / 1_000_000.0);
        System.out.println("==================================================================");
        System.out.printf("Total Successful Operations: %,d in %.3f seconds.%n", res.totalOps, res.durationSec);
        System.out.println("----------------------------------------------------------------==");
    }

    private static class BenchmarkResult {
        String phaseName;
        long totalOps;
        double durationSec;
        double opsSec;
        long[] latencies;

        public BenchmarkResult(String phaseName, long totalOps, double durationSec, double opsSec, long[] latencies) {
            this.phaseName = phaseName;
            this.totalOps = totalOps;
            this.durationSec = durationSec;
            this.opsSec = opsSec;
            this.latencies = latencies;
        }
    }

    /**
     * Simulation of the SWIM Cluster Membership + Distributed Vector Index Engine.
     */
    private static class SwimClusterSimulator {
        private final ClusterNode[] nodes;
        private final Random rand = new Random();

        public SwimClusterSimulator(int clusterSize, int vectorDimension) {
            this.nodes = new ClusterNode[clusterSize];
            for (int i = 0; i < clusterSize; i++) {
                nodes[i] = new ClusterNode("node-" + i, vectorDimension);
            }
        }

        public boolean routeAndSyncVector(String targetNodeId, float[] vector) {
            // Simulate metadata lookup / consistent hashing route
            int index = Math.abs(targetNodeId.hashCode()) % nodes.length;
            ClusterNode target = nodes[index];

            // Simulate SWIM gossip dissemination overhead & vector index insertion
            target.ingestVector(vector);

            // Simulate minor network/gossip propagation to a random peer
            ClusterNode peer = nodes[rand.nextInt(nodes.length)];
            if (peer != target) {
                peer.receiveGossip(target.id);
            }

            return true;
        }
    }

    private static class ClusterNode {
        String id;
        int vectorDimension;
        // Mock storage representing vector embeddings mapped in cluster shard
        float[][] vectorTable = new float[1024][];
        int ptr = 0;

        public ClusterNode(String id, int vectorDimension) {
            this.id = id;
            this.vectorDimension = vectorDimension;
        }

        public synchronized void ingestVector(float[] vector) {
            vectorTable[ptr = (ptr + 1) % vectorTable.length] = vector;
            // Simulate light compute (e.g., distance calculation or index lock acquisition)
            calculateMockNorm(vector);
        }

        public void receiveGossip(String peerId) {
            // Simulate processing heartbeat state vector
            int dummyHash = peerId.hashCode() ^ vectorDimension;
            if (dummyHash == 0) {
                Thread.yield();
            }
        }

        private float calculateMockNorm(float[] vec) {
            float sum = 0;
            for (float v : vec) {
                sum += v * v;
            }
            return (float) Math.sqrt(sum);
        }
    }

    private static class ThreadLocalRandomAdapter {
        private final Random random = new Random();

        public float[] nextVector(int dim) {
            float[] vec = new float[dim];
            for (int i = 0; i < dim; i++) {
                vec[i] = random.nextFloat();
            }
            return vec;
        }

        public int nextInt(int bound) {
            return random.nextInt(bound);
        }
    }
}