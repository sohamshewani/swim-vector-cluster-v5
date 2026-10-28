package swim.vector.cluster.types;

import java.util.Objects;

/**
 * Represents a unique identifier for a node within the SWIM vector cluster.
 * Designed for low-overhead storage, efficient off-heap serialization, and 
 * zero-allocation usage on hot critical paths.
 */
public final class NodeId implements Comparable<NodeId> {

    private final long mostSignificantBits;
    private final long leastSignificantBits;
    private final int cachedHashCode;

    /**
     * Constructs a NodeId from its 128-bit components.
     * 
     * @param mostSignificantBits the most significant 64 bits of the node identifier
     * @param leastSignificantBits the least significant 64 bits of the node identifier
     */
    public NodeId(long mostSignificantBits, long leastSignificantBits) {
        this.mostSignificantBits = mostSignificantBits;
        this.leastSignificantBits = leastSignificantBits;
        
        // Pre-compute hash code to eliminate runtime overhead in hash-based collections
        long hilo = mostSignificantBits ^ leastSignificantBits;
        this.cachedHashCode = (int) ((hilo >>> 32) ^ hilo);
    }

    /**
     * Creates a NodeId from an existing byte array starting at a given offset.
     * Useful for parsing NodeIds directly from off-heap memory or packet buffers
     * without intermediate object allocations.
     * 
     * @param buffer the source byte array containing the serialized NodeId
     * @param offset the starting index in the byte array
     * @return a constructed NodeId instance
     */
    public static NodeId fromBytes(byte[] buffer, int offset) {
        Objects.requireNonNull(buffer, "Buffer cannot be null");
        if (offset < 0 || offset + 16 > buffer.length) {
            throw new IndexOutOfBoundsException("Offset out of bounds for 16-byte NodeId read: offset=" + offset + ", length=" + buffer.length);
        }

        long msb = 0L;
        long lsb = 0L;

        for (int i = 0; i < 8; i++) {
            msb = (msb << 8) | (buffer[offset + i] & 0xFF);
        }
        for (int i = 0; i < 8; i++) {
            lsb = (lsb << 8) | (buffer[offset + 8 + i] & 0xFF);
        }

        return new NodeId(msb, lsb);
    }

    /**
     * Serializes this NodeId directly into a target byte array at the specified offset.
     * Optimized for zero-allocation packet assembly in the gossip engine.
     * 
     * @param destination the destination byte array
     * @param offset the starting index in the destination array
     */
    public void writeBytes(byte[] destination, int offset) {
        Objects.requireNonNull(destination, "Destination buffer cannot be null");
        if (offset < 0 || offset + 16 > destination.length) {
            throw new IndexOutOfBoundsException("Offset out of bounds for 16-byte NodeId write: offset=" + offset + ", length=" + destination.length);
        }

        long msb = this.mostSignificantBits;
        long lsb = this.leastSignificantBits;

        for (int i = 7; i >= 0; i--) {
            destination[offset + i] = (byte) (msb & 0xFF);
            msb >>>= 8;
        }
        for (int i = 7; i >= 0; i--) {
            destination[offset + 8 + i] = (byte) (lsb & 0xFF);
            lsb >>>= 8;
        }
    }

    /**
     * Returns the most significant 64 bits of the node identifier.
     * 
     * @return the most significant bits
     */
    public long getMostSignificantBits() {
        return this.mostSignificantBits;
    }

    /**
     * Returns the least significant 64 bits of the node identifier.
     * 
     * @return the least significant bits
     */
    public long getLeastSignificantBits() {
        return this.leastSignificantBits;
    }

    @Override
    public int hashCode() {
        return this.cachedHashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        NodeId other = (NodeId) obj;
        return this.mostSignificantBits == other.mostSignificantBits 
                && this.leastSignificantBits == other.leastSignificantBits;
    }

    @Override
    public int compareTo(NodeId other) {
        Objects.requireNonNull(other, "Comparison target cannot be null");
        if (this.mostSignificantBits != other.mostSignificantBits) {
            return Long.compareUnsigned(this.mostSignificantBits, other.mostSignificantBits);
        }
        return Long.compareUnsigned(this.leastSignificantBits, other.leastSignificantBits);
    }

    @Override
    String toString() {
        StringBuilder sb = new StringBuilder(36);
        appendHex(sb, this.mostSignificantBits >>> 32, 8);
        sb.append('-');
        appendHex(sb, (this.mostSignificantBits >>> 16) & 0xFFFF, 4);
        sb.append('-');
        appendHex(sb, this.mostSignificantBits & 0xFFFF, 4);
        sb.append('-');
        appendHex(sb, this.leastSignificantBits >>> 48, 4);
        sb.append('-');
        appendHex(sb, this.leastSignificantBits & 0xFFFFFFFFFFFFL, 12);
        return sb.toString();
    }

    private static void appendHex(StringBuilder sb, long val, int digits) {
        String hex = Long.toHexString(val);
        for (int i = hex.length(); i < digits; i++) {
            sb.append('0');
        }
        sb.append(hex);
    }
}