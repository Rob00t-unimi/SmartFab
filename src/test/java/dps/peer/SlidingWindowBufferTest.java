package dps.peer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sensor.Measurement;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class SlidingWindowBufferTest {
    private SlidingWindowBuffer buffer;

    @BeforeEach
    void setUp() {
        buffer = new SlidingWindowBuffer();
    }

    @Test
    void testFirstWindow() {
        for (int i = 0; i < 8; i++) {
            buffer.addMeasurement(new Measurement("peer1", "vibration", i, System.currentTimeMillis()));
        }

        List<Measurement> result = buffer.readAllAndClear();
        assertEquals(8, result.size(), "First window should have 8 measurements");
        assertEquals(0.0, result.get(0).value());
        assertEquals(7.0, result.get(7).value());
    }

    @Test
    void testOverlapBetweenReads() {
        // Fill first window
        for (int i = 0; i < 8; i++) {
            buffer.addMeasurement(new Measurement("peer1", "vibration", i, System.currentTimeMillis()));
        }
        buffer.readAllAndClear(); // Removes 0,1,2,3. Retains 4,5,6,7.

        // Add 4 more to complete next window
        for (int i = 8; i < 12; i++) {
            buffer.addMeasurement(new Measurement("peer1", "vibration", i, System.currentTimeMillis()));
        }

        List<Measurement> result = buffer.readAllAndClear();
        assertEquals(8, result.size(), "Second window should have 8 measurements");
        // Expected: [4, 5, 6, 7, 8, 9, 10, 11]
        assertEquals(4.0, result.get(0).value());
        assertEquals(11.0, result.get(7).value());
    }

    @Test
    void testClear() {
        for (int i = 0; i < 4; i++) {
            buffer.addMeasurement(new Measurement("peer1", "vibration", i, System.currentTimeMillis()));
        }
        buffer.clear();
        
        Thread t = new Thread(() -> buffer.readAllAndClear());
        t.start();
        
        try {
            Thread.sleep(200);
            assertTrue(t.isAlive(), "Thread should be waiting because buffer was cleared");
            t.interrupt();
        } catch (InterruptedException e) {
            fail("Test interrupted");
        }
    }

    @Test
    void testReadWhenNotEnoughDataBlocks() {
        for (int i = 0; i < 7; i++) {
            buffer.addMeasurement(new Measurement("peer1", "vibration", i, System.currentTimeMillis()));
        }

        Thread consumerThread = new Thread(() -> buffer.readAllAndClear());
        consumerThread.start();

        try {
            Thread.sleep(200);
            assertTrue(consumerThread.isAlive(), "Thread should be waiting for the 8th measurement");
            
            buffer.addMeasurement(new Measurement("peer1", "vibration", 7.0, System.currentTimeMillis()));
            
            Thread.sleep(200);
            assertFalse(consumerThread.isAlive(), "Thread should have finished after getting 8th measurement");
        } catch (InterruptedException e) {
            fail("Test interrupted");
        }
    }

    @Test
    void testBufferMaxCapacityDrop() {
        // Capacity is 8. Adding 10 should drop the last 2.
        for (int i = 0; i < 10; i++) {
            buffer.addMeasurement(new Measurement("peer1", "vibration", i, System.currentTimeMillis()));
        }

        List<Measurement> result = buffer.readAllAndClear();
        assertEquals(8, result.size());
        // Last element should be 7.0 (8.0 and 9.0 were dropped)
        assertEquals(7.0, result.get(7).value());
    }
}
