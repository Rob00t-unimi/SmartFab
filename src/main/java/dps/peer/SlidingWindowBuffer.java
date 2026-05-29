package dps.peer;

import sensor.Buffer;
import sensor.Measurement;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of a sliding window buffer with a size of 8 and 50% overlap.
 * Uses standard Java synchronization (synchronized, wait, notifyAll).
 */
public class SlidingWindowBuffer implements Buffer {
    private final List<Measurement> measurements = new ArrayList<>();
    private static final int WINDOW_SIZE = 8;
    private static final int OVERLAP_STEP = 4;
    private static final int MAX_CAPACITY = 100;

    @Override
    public synchronized void addMeasurement(Measurement m) {
        if (measurements.size() >= MAX_CAPACITY) {
            // Drop new measurement if buffer is full to preserve real-time sensor behavior
            return;
        }
        
        measurements.add(m);
        // Notify any waiting consumer that new data is available
        if (measurements.size() >= WINDOW_SIZE) {
            notifyAll();
        }
    }

    @Override
    public synchronized List<Measurement> readAllAndClear() {
        // Wait until at least one full window is available
        while (measurements.size() < WINDOW_SIZE) {
            try {
                wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new ArrayList<>();
            }
        }

        List<Measurement> result = new ArrayList<>();
        // Process all available windows
        while (measurements.size() >= WINDOW_SIZE) {
            // Add the current window (8 measurements) to the result list
            result.addAll(new ArrayList<>(measurements.subList(0, WINDOW_SIZE)));

            // Slide the window by removing the first 4 measurements (50% overlap)
            for (int i = 0; i < OVERLAP_STEP; i++) {
                measurements.remove(0);
            }
        }

        return result;
    }

    @Override
    public synchronized void clear() {
        measurements.clear();
        notifyAll();
    }
}
