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
        // Adding exactly 8 measurements
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
        // First read
        for (int i = 0; i < 8; i++) {
            buffer.addMeasurement(new Measurement("peer1", "vibration", i, System.currentTimeMillis()));
        }
        buffer.readAllAndClear();

        // After readAllAndClear, the buffer should retain 4, 5, 6, 7
        // We add 4 more: 8, 9, 10, 11
        for (int i = 8; i < 12; i++) {
            buffer.addMeasurement(new Measurement("peer1", "vibration", i, System.currentTimeMillis()));
        }

        List<Measurement> result = buffer.readAllAndClear();
        assertEquals(8, result.size(), "Second window should also have 8 measurements");
        // Window should be [4, 5, 6, 7, 8, 9, 10, 11]
        assertEquals(4.0, result.get(0).value());
        assertEquals(11.0, result.get(7).value());
    }

    @Test
    void testMultipleWindowsAtOnce() {
        // Adding 12 measurements should produce 2 windows: (0-7) and (4-11)
        for (int i = 0; i < 12; i++) {
            buffer.addMeasurement(new Measurement("peer1", "vibration", i, System.currentTimeMillis()));
        }

        List<Measurement> result = buffer.readAllAndClear();
        // 2 windows * 8 measurements = 16 elements in the flat list
        assertEquals(16, result.size());
        
        // Check first window
        assertEquals(0.0, result.get(0).value());
        assertEquals(7.0, result.get(7).value());
        
        // Check second window (starts at index 8)
        assertEquals(4.0, result.get(8).value());
        assertEquals(11.0, result.get(15).value());
    }

    @Test
    void testClear() {
        for (int i = 0; i < 4; i++) {
            buffer.addMeasurement(new Measurement("peer1", "vibration", i, System.currentTimeMillis()));
        }
        buffer.clear();
        
        // Use a thread to check that it blocks (since it's empty)
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
    void testOverlapWithMultipleWindows() {
        // 1. Initial 8 measurements
        for (int i = 0; i < 8; i++) {
            buffer.addMeasurement(new Measurement("peer1", "vibration", i, System.currentTimeMillis()));
        }
        buffer.readAllAndClear(); // Buffer retains [4, 5, 6, 7]

        // 2. Add 8 more: 8, 9, 10, 11, 12, 13, 14, 15
        // Buffer now has 12 elements: [4...15]
        for (int i = 8; i < 16; i++) {
            buffer.addMeasurement(new Measurement("peer1", "vibration", i, System.currentTimeMillis()));
        }

        List<Measurement> result = buffer.readAllAndClear();
        
        // Should produce 2 windows: [4-11] and [8-15]
        // Total size = 2 * 8 = 16
        assertEquals(16, result.size());

        // Window 1: 4 to 11
        assertEquals(4.0, result.get(0).value());
        assertEquals(11.0, result.get(7).value());

        // Window 2: 8 to 15
        assertEquals(8.0, result.get(8).value());
        assertEquals(15.0, result.get(15).value());
    }

    @Test
    void testReadWhenNotEnoughDataBlocks() {
        // Adding only 7 measurements (less than WINDOW_SIZE = 8)
        for (int i = 0; i < 7; i++) {
            buffer.addMeasurement(new Measurement("peer1", "vibration", i, System.currentTimeMillis()));
        }

        Thread consumerThread = new Thread(() -> buffer.readAllAndClear());
        consumerThread.start();

        try {
            // Give it some time to start and enter the wait() state
            Thread.sleep(200);
            assertTrue(consumerThread.isAlive(), "Thread should be waiting for the 8th measurement");
            
            // Now add the 8th measurement
            buffer.addMeasurement(new Measurement("peer1", "vibration", 7.0, System.currentTimeMillis()));
            
            // Give it time to wake up and finish
            Thread.sleep(200);
            assertFalse(consumerThread.isAlive(), "Thread should have finished after getting 8th measurement");
        } catch (InterruptedException e) {
            fail("Test interrupted");
        }
    }

    @Test
    void testReadWithPartialNextWindow() {
        // Adding 10 measurements:
        // Window 1: [0, 1, 2, 3, 4, 5, 6, 7]
        // Remaining after slide: [4, 5, 6, 7, 8, 9] (size 6, which is < 8)
        for (int i = 0; i < 10; i++) {
            buffer.addMeasurement(new Measurement("peer1", "vibration", i, System.currentTimeMillis()));
        }

        List<Measurement> result = buffer.readAllAndClear();
        
        // Should only return one window (8 elements)
        assertEquals(8, result.size(), "Should only return one complete window");
        assertEquals(0.0, result.get(0).value());
        assertEquals(7.0, result.get(7).value());

        // Now add 2 more measurements (6 existing + 2 new = 8)
        for (int i = 10; i < 12; i++) {
            buffer.addMeasurement(new Measurement("peer1", "vibration", i, System.currentTimeMillis()));
        }

        List<Measurement> result2 = buffer.readAllAndClear();
        assertEquals(8, result2.size(), "Should now return the second complete window");
        assertEquals(4.0, result2.get(0).value());
        assertEquals(11.0, result2.get(7).value());
    }

    @Test
    void testBufferMaxCapacityDrop() {
        // We add 105 measurements, but MAX_CAPACITY is set to 100 in the buffer.
        // The policy is "Silent Discard" (Drop new data if the buffer is full).
        for (int i = 0; i < 105; i++) {
            buffer.addMeasurement(new Measurement("peer1", "vibration", i, System.currentTimeMillis()));
        }

        List<Measurement> result = buffer.readAllAndClear();
        
        /* 
         * Calculation for 100 elements (indexed 0 to 99):
         * WINDOW_SIZE = 8, STEP = 4.
         * The n-th window ends at index: (n-1) * STEP + WINDOW_SIZE - 1
         * We find the maximum n such that: (n-1) * 4 + 7 <= 99
         * (n-1) * 4 <= 92  =>  n-1 <= 23  =>  n <= 24.
         * 
         * Exactly 24 complete windows can be formed.
         * Total elements in the flat list = 24 windows * 8 elements = 192.
         */
        assertEquals(192, result.size(), "Buffer should contain exactly 24 windows (192 elements) after dropping data beyond 100");
        
        /*
         * Since we dropped measurements from 100 to 104, the very last measurement 
         * in the last window must be the one with value 99.0.
         */
        assertEquals(99.0, result.get(result.size() - 1).value(), "The last stored value should be 99.0 (values 100-104 should be dropped)");
    }
}
