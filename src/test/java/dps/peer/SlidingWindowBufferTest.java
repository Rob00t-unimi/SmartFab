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
}
