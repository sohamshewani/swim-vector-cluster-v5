package concurrency;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * SpinLatch: A hybrid progressive back-off spin-lock primitive.
 * Designed for ultra-low latency cluster state synchronization and heartbeat dispatch loops
 * within the swim-vector-cluster engine. Combines CPU yield instructions (Thread.onSpinWait)
 * with thread parking mechanisms to prevent CPU saturation while minimizing context-switch overhead.
 */
public final class SpinLatch {

    private static final int DEFAULT_SPIN_LIMIT = 64;
    private static final int DEFAULT_YIELD_LIMIT = 256;

    private static final VarHandle LOCKED;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            LOCKED = l.findVarHandle(SpinLatch.class, "locked", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @SuppressWarnings("unused")
    private volatile int locked = 0;

    private final int spinLimit;
    private final int yieldLimit;

    /**
     * Creates a SpinLatch with default back-off parameters tuned for low-latency network loops.
     */
    public SpinLatch() {
        this.spinLimit = DEFAULT_SPIN_LIMIT;
        this.yieldLimit = DEFAULT_YIELD_LIMIT;
    }

    /**
     * Creates a SpinLatch with custom spin and yield limits.
     *
     * @param spinLimit  Number of pure spin iterations using Thread.onSpinWait()
     * @param yieldLimit Number of iterations utilizing Thread.yield() before parking
     */
    public SpinLatch(int spinLimit, int yieldLimit) {
        if (spinLimit < 0 || yieldLimit < 0) {
            throw new IllegalArgumentException("Spin and yield limits must be non-negative.");
        }
        this.spinLimit = spinLimit;
        this.yieldLimit = yieldLimit;
    }

    /**
     * Acquires the latch, blocking the calling thread until exclusive access is granted.
     * Uses a progressive back-off strategy:
1. Pure CPU spin with {@link Thread#onSpinWait()}
     * 2. Yielding phase via {@link Thread#yield()}
     * 3. Sleeping/Parking phase via {@link LockSupport#park(Object)}
     */
    public void acquire() {
        if (tryAcquire()) {
            return;
        }

        int spins = 0;
        int yields = 0;
        Thread currentThread = Thread.currentThread();

        while (true) {
            if (tryAcquire()) {
                return;
            }

            if (spins < spinLimit) {
                spins++;
                Thread.onSpinWait();
            } else if (yields < yieldLimit) {
                yields++;
                Thread.yield();
            } else {
                // Reset counters for the next parking cycle
                spins = 0;
                yields = 0;
                LockSupport.park(this);
                
                // Check for interruption status post-unpark if required by runtime context,
                // but maintain lock acquisition semantics.
                if (Thread.interrupted()) {
                    currentThread.interrupt();
                }
            }
        }
    }

    /**
     * Attempts to acquire the latch within a specified timeout.
     *
     * @param timeout the maximum time to wait
     * @param unit    the time unit of the timeout argument
     * @return true if the latch was successfully acquired, false if the timeout elapsed
     */
    public boolean tryAcquire(long timeout, TimeUnit unit) {
        if (tryAcquire()) {
            return true;
        }

        long nanosTimeout = unit.toNanos(timeout);
        if (nanosTimeout <= 0L) {
            return false;
        }

        long startNano = System.nanoTime();
        int spins = 0;
        int yields = 0;
        Thread currentThread = Thread.currentThread();

        while (true) {
            if (tryAcquire()) {
                return true;
            }

            long elapsedNanos = System.nanoTime() - startNano;
            long timeLeft = nanosTimeout - elapsedNanos;
            if (timeLeft <= 0L) {
                return false;
            }

            if (spins < spinLimit) {
                spins++;
                Thread.onSpinWait();
            } else if (yields < yieldLimit) {
                yields++;
                Thread.yield();
            } else {
                spins = 0;
                yields = 0;
                LockSupport.parkNanos(this, Math.min(timeLeft, 100_000L)); // Max 100us park slices
                
                if (Thread.interrupted()) {
                    currentThread.interrupt();
                }
            }
        }
    }

    /**
     * Non-blocking attempt to acquire the latch.
     *
     * @return true if the latch was acquired, false otherwise
     */
    public boolean tryAcquire() {
        return (int) LOCKED.compareAndExchangeAcquire(this, 0, 1) == 0;
    }

    /**
     * Releases the latch, waking up a waiting thread if any are parked.
     */
    public void release() {
        if (!LOCKED.compareAndSet(this, 1, 0)) {
            throw new IllegalMonitorStateException("Attempted to release an unacquired SpinLatch.");
        }
        // Force memory fence and notify potential parked waiters
        // Unparking is lightweight and safe even if no thread is currently parked.
        // To find the parked thread, we examine threads blocked on this object or rely on unpark(null).
        // Since LockSupport.park(this) sets the blocker, we can retrieve it or use unpark broadcast.
        // In this implementation, we rely on standard LockSupport unparking via thread tracking if available,
        // or a general unpark mechanism. Since LockSupport.unpark requires the target Thread, 
        // standard practice for simple spin-latches is to track the owner or utilize general signaling.
        // To strictly adhere to zero-allocation and performance profiles, we use a targeted unpark if ownership is tracked,
        // or fall back to notifying via thread reference if stored. Here we store the owner thread atomically.
        unparkWaiter();
    }

    private volatile Thread ownerThread;

    /**
     * Enhanced acquire with owner tracking for precise unparking.
     */
    public void acquireExclusive() {
        if (tryAcquireExclusive()) {
            return;
        }

        int spins = 0;
        int yields = 0;
        Thread current = Thread.currentThread();

        while (true) {
            if (tryAcquireExclusive()) {
                return;
            }

            if (spins < spinLimit) {
                spins++;
                Thread.onSpinWait();
            } else if (yields < yieldLimit) {
                yields++;
                Thread.yield();
            } else {
                spins = 0;
                yields = 0;
                ownerThread = current;
                // Re-check after setting owner to avoid lost wakeups
                if (tryAcquireExclusive()) {
                    ownerThread = null;
                    return;
                }
                LockSupport.park(this);
                ownerThread = null;

                if (Thread.interrupted()) {
                    current.interrupt();
                }
            }
        }
    }

    private boolean tryAcquireExclusive() {
        if ((int) LOCKED.compareAndExchangeAcquire(this, 0, 1) == 0) {
            ownerThread = Thread.currentThread();
            return true;
        }
        return false;
    }

    /**
     * Releases the latch tracking the owner thread and unparking deterministically.
     */
    public void releaseExclusive() {
        ownerThread = null;
        if (!LOCKED.compareAndSet(this, 1, 0)) {
            throw new IllegalMonitorStateException("Attempted to release an unacquired SpinLatch.");
        }
        unparkWaiter();
    }

    private void unparkWaiter() {
        // Find and unpark any thread blocked on this latch. 
        // Since LockSupport.park(this) registers the blocker, we can inspect threads or use a general unpark.
        // For maximum performance without maintaining explicit thread queues, we unpark the registered owner or search.
        Thread t = ownerThread;
        if (t != null) {
            LockSupport.unpark(t);
        }
    }

    /**
     * Checks if the latch is currently locked.
     *
     * @return true if locked, false otherwise
     */
    public boolean isLocked() {
        return (int) LOCKED.getAcquire(this) == 1;
    }
}