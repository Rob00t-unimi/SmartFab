package dps.peer.sensor;

import sensor.Buffer;
import sensor.Measurement;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of a sliding window buffer with a size of 8 and 50% overlap.
 */
public class SlidingWindowBuffer implements Buffer {

    private static final int WINDOW_SIZE = 8;
    private static final int OVERLAP_STEP = WINDOW_SIZE / 2;

    private final List<Measurement> measurements = new ArrayList<>();


    /** Method invoked by the sensor thread (producer) to insert a measurement. **/
    @Override
    public synchronized void addMeasurement(Measurement m) {
        // Wait if the buffer is full (reaches WINDOW_SIZE)
        // This effectively pauses the sensor thread until space is available.
        while (measurements.size() >= WINDOW_SIZE) {
            try {
                wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        measurements.add(m);

        // Notify readers if the window is now complete
        if (measurements.size() >= WINDOW_SIZE) {
            notifyAll();
        }
    }

    /** Method invoked by the monitoring loop (consumer) **/
    @Override
    public synchronized List<Measurement> readAllAndClear() {
        // Wait until the window is full
        while (measurements.size() < WINDOW_SIZE) {
            try {
                wait();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new ArrayList<>(); // returns an empty list if interrupted
            }
        }

        // Return the current window
        List<Measurement> result = new ArrayList<>(measurements.subList(0, WINDOW_SIZE));

        // Slide: remove the first OVERLAP_STEP elements (50% overlap)
        for (int i = 0; i < OVERLAP_STEP; i++) {
            measurements.remove(0);
        }

        // Notify the producer (sensor) that there is now space in the buffer
        notifyAll();

        return result;
    }

    /**
     * Completely empties the buffer (called during alarm/calibration)
     * wakes up all threads to prevent deadlocks during state changes.
     **/
    @Override
    public synchronized void clear() {
        measurements.clear();
        notifyAll();
    }
}
