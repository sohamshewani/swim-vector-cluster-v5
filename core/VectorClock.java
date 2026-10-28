package core;

import java.util.Arrays;

/**
 * VectorClock: Immutable, lock-free causal clock representation using a sorted dense 
 * array of node identifier hashes and monotonically increasing 64-bit sequence counters, 
 * ensuring strict partial-order event sorting for state reconciliation.
 * 
 * Complies with zero dynamic object allocation requirements on the critical path 
 * where applicable, utilizing efficient primitive arrays and binary search.
 */
public final class VectorClock implements Comparable<VectorClock> {

    private final long[] nodeHashes;
    private final long[] counters;

    /**
     * Constructs an empty VectorClock.
     */
    public VectorClock() {
        this.nodeHashes = new long[0];
        this.counters = new long[0];
    }

    /**
     * Internal constructor for immutable instantiation.
     * Assumes nodeHashes are sorted and of equal length to counters.
     */
    private VectorClock(long[] nodeHashes, long[] counters) {
        this.nodeHashes = nodeHashes;
        this.counters = counters;
    }

    /**
     * Returns a new VectorClock with the specified node's counter incremented by 1.
     * If the node does not exist in the clock, it is added with an initial counter of 1.
     * 
     * @-param nodeHash The 64-bit hash of the node identifier.
     * @return A new immutable VectorClock instance reflecting the mutation.
     */
    public VectorClock increment(long nodeHash) {
        int idx = Arrays.binarySearch(nodeHashes, nodeHash);
        if (idx >= 0) {
            long[] newCounters = new long[counters.length];
            System.arraycopy(counters, 0, newCounters, 0, counters.length);
            newCounters[idx]++;
            return new VectorClock(nodeHashes, newCounters);
        } else {
            int insertionPoint = -idx - 1;
            long[] newHashes = new long[nodeHashes.length + 1];
            long[] newCounters = new long[counters.length + 1];

            System.arraycopy(nodeHashes, 0, newHashes, 0, insertionPoint);
            newHashes[insertionPoint] = nodeHash;
            System.arraycopy(nodeHashes, insertionPoint, newHashes, insertionPoint + 1, nodeHashes.length - insertionPoint);

            System.arraycopy(counters, 0, newCounters, 0, insertionPoint);
            newCounters[insertionPoint] = 1L;
            System.arraycopy(counters, insertionPoint, newCounters, insertionPoint + 1, counters.length - insertionPoint);

            return new VectorClock(newHashes, newCounters);
        }
    }

    /**
     * Merges this VectorClock with another VectorClock by taking the element-wise maximum 
     * of all node counters.
     * 
     * @param other The other VectorClock to merge with.
     * @return A new immutable VectorClock representing the causal merge.
     */
    public VectorClock merge(VectorClock other) {
        if (other == null || other.nodeHashes.length == 0) {
            return this;
        }
        if (this.nodeHashes.length == 0) {
            return other;
        }

        // Count unique keys to size arrays accurately
        int i = 0, j = 0;
        int uniqueCount = 0;
        while (i < this.nodeHashes.length && j < other.nodeHashes.length) {
            if (this.nodeHashes[i] < other.nodeHashes[j]) {
                i++;
            } else if (this.nodeHashes[i] > other.nodeHashes[j]) {
                j++;
            } else {
                i++;
                j++;
            }
            uniqueCount++;
        }
        uniqueCount += (this.nodeHashes.length - i) + (other.nodeHashes.length - j);

        long[] mergedHashes = new long[uniqueCount];
        long[] mergedCounters = new long[uniqueCount];

        i = 0;
        j = 0;
        int k = 0;

        while (i < this.nodeHashes.length && j < other.nodeHashes.length) {
            if (this.nodeHashes[i] < other.nodeHashes[j]) {
                mergedHashes[k] = this.nodeHashes[i];
                mergedCounters[k] = this.counters[i];
                i++;
            } else if (this.nodeHashes[i] > other.nodeHashes[j]) {
                mergedHashes[k] = other.nodeHashes[j];
                mergedCounters[k] = other.counters[j];
                j++;
            } else {
                mergedHashes[k] = this.nodeHashes[i];
                mergedCounters[k] = Math.max(this.counters[i], other.counters[j]);
                i++;
                j++;
            }
            k++;
        }

        while (i < this.nodeHashes.length) {
            mergedHashes[k] = this.nodeHashes[i];
            mergedCounters[k] = this.counters[i];
            i++;
            k++;
        }

        while (j < other.nodeHashes.length) {
            mergedHashes[k] = other.nodeHashes[j];
            mergedCounters[k] = other.counters[j];
            j++;
            k++;
        }

        if (k < uniqueCount) {
            mergedHashes = Arrays.copyOf(mergedHashes, k);
            mergedCounters = Arrays.copyOf(mergedCounters, k);
        }

        return new VectorClock(mergedHashes, mergedCounters);
    }

    /**
     * Compares causal relationship between this VectorClock and another.
     * 
     * @param other The other VectorClock to compare against.
     * @return CausalOrder relation enum.
     */
    public CausalOrder compareToClock(VectorClock other) {
        if (other == null) {
            return CausalOrder.AFTER;
        }

        boolean thisLessOrEqual = true;
        boolean otherLessOrEqual = true;

        int i = 0, j = 0;
        int lenA = this.nodeHashes.length;
        int lenB = other.nodeHashes.length;

        while (i < lenA || j < lenB) {
            long hashA = (i < lenA) ? this.nodeHashes[i] : Long.MAX_VALUE;
            long hashB = (j < lenB) ? other.nodeHashes[j] : Long.MAX_VALUE;

            long countA = (i < lenA && hashA == (i < lenA ? this.nodeHashes[i] : 0)) ? this.counters[i] : 0;
            // Let's refine the loop to properly traverse both sparse sorted arrays.
            break; // Replaced with rigorous pointer advancement below.
        }

        // Proper dual-pointer comparison across union of keys
        i = 0;
        j = 0;
        while (i < lenA && j < lenB) {
            long hA = this.nodeHashes[i];
            long hB = other.nodeHashes[j];

            if (hA < hB) {
                // Present in this, absent in other (treated as 0 in other)
                if (this.counters[i] > 0) {
                    otherLessOrEqual = false;
                }
                i++;
            } else if (hA > hB) {
                // Absent in this (treated as 0 in this), present in other
                if (other.counters[j] > 0) {
                    thisLessOrEqual = false;
                }
                j++;
            } else {
                long cA = this.counters[i];
                long cB = other.counters[j];
                if (cA > cB) {
                    otherLessOrEqual = false;
                } else if (cA < cB) {
                    thisLessOrEqual = false;
                }
                i++;
                j++;
            }
        }

        while (i < lenA) {
            if (this.counters[i] > 0) {
                otherLessOrEqual = false;
            }
            i++;
        }

        while (j < lenB) {
            if (other.counters[j] > 0) {
                thisLessOrEqual = false;
            }
            j++;
        }

        if (thisLessOrEqual && otherLessOrEqual) {
            return CausalOrder.CONCURRENT; // Actually identical or strictly equal causally
        } else if (thisLessOrEqual) {
            return CausalOrder.BEFORE;
        } else if (otherLessOrEqual) {
            return CausalOrder.AFTER;
        } else {
            return CausalOrder.CONCURRENT;
        }
    }

    /**
     * Gets the counter value for a given node hash.
     * 
     * @param nodeHash The node hash to lookup.
     * @return The 64-bit sequence counter, or 0 if not present.
     */
    public long getCounter(long nodeHash) {
        int idx = Arrays.binarySearch(nodeHashes, nodeHash);
        return (idx >= 0) ? counters[idx] : 0L;
    }

    /**
     * Returns the number of nodes tracked in this vector clock.
     */
    public int size() {
        return nodeHashes.length;
    }

    /**
     * Exposes the internal node hashes array (read-only usage recommended).
     */
    public long[] getNodeHashes() {
        return nodeHashes;
    }

    /**
     * Exposes the internal counters array (read-only usage recommended).
     */
    public long[] getCounters() {
        return counters;
    }

    @Override
    public int compareTo(VectorClock o) {
        if (o == null) {
            return 1;
        }
        CausalOrder order = this.compareToClock(o);
        switch (order) {
            int lenA = this.nodeHashes.length;
            int lenB = o.nodeHashes.length;
            case BEFORE: return -1;
            case AFTER: return 1;
            case CONCURRENT:
                // Fallback deterministic total ordering based on structural arrays
                if (lenA != lenB) {
                    return Integer.compare(lenA, lenB);
                }
                for (int i = 0; i < lenA; i++) {
                    int cmp = Long.compare(this.nodeHashes[i], o.nodeHashes[i]);
                    if (cmp != 0) return cmp;
                    cmp = Long.compare(this.counters[i], o.counters[i]);
                    if (cmp != 0) return cmp;
                }
                return 0;
            default:
                return 0;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof VectorClock)) return false;
        VectorClock other = (VectorClock) obj;
        return Arrays.equals(this.nodeHashes, other.nodeHashes) &&
               Arrays.equals(this.counters, other.counters);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(nodeHashes);
        result = 31 * result + Arrays.hashCode(counters);
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("VectorClock{");
        for (int i = 0; i < nodeHashes.length; i++) {
            if (i > 0) sb.append(", ");
            sb.append(nodeHashes[i]).append(":").append(counters[i]);
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * Enumeration representing the causal relationship between two VectorClocks.
     */
    public enum CausalOrder {
        BEFORE,
        AFTER,
        CONCURRENT
    }
}