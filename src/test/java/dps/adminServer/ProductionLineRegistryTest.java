package dps.adminServer;

import dps.common.model.OperationalState;
import dps.common.model.ProductionLine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

public class ProductionLineRegistryTest {

    private ProductionLineRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new ProductionLineRegistry();
    }

    @Test
    void testFineGrainedSynchronization() throws InterruptedException {
        int lineId1 = 1;
        int lineId2 = 2;
        registry.register(new ProductionLine(lineId1, "127.0.0.1", 5001));
        registry.register(new ProductionLine(lineId2, "127.0.0.1", 5002));

        // Fill line 1 with a lot of data to make getAverageVibration "slower" (relatively)
        for (int i = 0; i < 10000; i++) {
            registry.addTelemetry(lineId1, i, System.currentTimeMillis() + i);
        }

        AtomicBoolean line2Updated = new AtomicBoolean(false);
        AtomicLong updateTime = new AtomicLong(0);

        // Thread A: performs a heavy calculation on Line 1
        Thread threadA = new Thread(() -> {
            registry.getAverageVibration(lineId1, 0, Long.MAX_VALUE);
        });

        // Thread B: updates state of Line 2
        Thread threadB = new Thread(() -> {
            long start = System.currentTimeMillis();
            registry.updateState(lineId2, OperationalState.UNDER_CALIBRATION);
            updateTime.set(System.currentTimeMillis() - start);
            line2Updated.set(true);
        });

        threadA.start();
        // Delay slightly to ensure threadA has potentially grabbed some lock
        Thread.sleep(10); 
        threadB.start();

        threadA.join();
        threadB.join();

        assertTrue(line2Updated.get(), "Line 2 should be updated even if Line 1 is being processed");
        // If the lock was coarse-grained (on 'this'), threadB would have to wait for threadA's 10000 entries copy.
        // In a fine-grained scenario, updateTime should be very low (near 0ms).
        assertTrue(updateTime.get() < 100, "Update on Line 2 was blocked for too long! (Fine-grained sync failure)");
    }

    @Test
    void testConcurrentRegistrations() throws InterruptedException {
        int threadCount = 50;
        Thread[] threads = new Thread[threadCount];
        AtomicBoolean failed = new AtomicBoolean(false);

        for (int i = 0; i < threadCount; i++) {
            final int id = i + 100;
            threads[i] = new Thread(() -> {
                try {
                    registry.register(new ProductionLine(id, "127.0.0.1", 6000 + id));
                } catch (Exception e) {
                    failed.set(true);
                }
            });
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        assertFalse(failed.get(), "Concurrent registrations should not fail or cause deadlocks");
        assertEquals(threadCount, registry.getAllLines().size());
    }
}
