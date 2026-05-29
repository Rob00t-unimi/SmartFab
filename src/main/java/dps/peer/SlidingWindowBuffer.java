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

    private static final int WINDOW_SIZE = 8;
    private static final int OVERLAP_STEP = 4;

    private final List<Measurement> measurements = new ArrayList<>();

    @Override
    public synchronized void addMeasurement(Measurement m) {
        if (measurements.size() >= WINDOW_SIZE) {
            // Drop new measurement if window is full
            return;
        }

        measurements.add(m);
        // Notify if window is now complete
        if (measurements.size() == WINDOW_SIZE) {
            notifyAll();
        }
    }

    @Override
    public synchronized List<Measurement> readAllAndClear() {
        // Wait until the window is full
        while (measurements.size() < WINDOW_SIZE) {
            try {
                wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new ArrayList<>();
            }
        }

        // Return the current window
        List<Measurement> result = new ArrayList<>(measurements);

        // Slide: remove the first 4 (50% overlap)
        for (int i = 0; i < OVERLAP_STEP; i++) {
            measurements.remove(0);
        }

        return result;
    }

    @Override
    public synchronized void clear() {
        measurements.clear();
        notifyAll();
    }
}
