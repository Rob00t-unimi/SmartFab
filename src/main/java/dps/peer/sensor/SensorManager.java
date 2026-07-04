package dps.peer.sensor;

import dps.common.model.OperationalState;
import dps.common.model.ProductionLine;
import dps.peer.ProductionLineNode;
import sensor.Measurement;
import sensor.MonitoringSensor;
import java.util.List;

/**
 * Manages the physical monitoring sensor and the background thread
 * consuming measurements from the sliding window buffer.
 */
public class SensorManager {

    private final ProductionLineNode node;
    private final ProductionLine self;
    private final double vibrationThreshold;

    private final SlidingWindowBuffer buffer = new SlidingWindowBuffer();
    private final MonitoringSensor sensor = new MonitoringSensor(buffer);

    private Thread monitoringThread;
    private volatile boolean running = false;

    public SensorManager(ProductionLineNode node, ProductionLine self, double vibrationThreshold) {
        this.node = node;
        this.self = self;
        this.vibrationThreshold = vibrationThreshold;
    }

    public SlidingWindowBuffer getBuffer() {
        return buffer;
    }

    public MonitoringSensor getSensor() {
        return sensor;
    }

    /**
     * Checks if the monitoring thread loop is configured to run.
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Pauses the physical sensor simulator.
     */
    public void pauseMeasuring() {
        sensor.pauseMeasuring();
    }

    /**
     * Resumes the physical sensor simulator.
     */
    public void startMeasuring() {
        sensor.startMeasuring();
    }

    /**
     * Clears the sliding window buffer.
     */
    public void clearBuffer() {
        buffer.clear();
    }

    /**
     * Computes the arithmetic average value of measurements in the window.
     */
    private double computeAverage(List<Measurement> window) {
        double sum = 0;
        for (Measurement m : window) {
            sum += m.value();
        }
        return sum / window.size();
    }

    /**
     * Starts the physical sensor simulator and the background thread that consumes
     * measurements from the sliding window buffer. Starts mqtt publishing.
     */
    public synchronized void startMonitoring() {
        if (monitoringThread != null) {
            return;
        }

        running = true;
        sensor.startMeasuring();    // start sensor simulation
        System.out.println("[PEER " + self.id() + "] Physical sensor simulator started.");

        monitoringThread = new Thread(() -> {
            while (running) {
                processSensorWindow();
            }
        });
        monitoringThread.setName("Sensor-Monitoring-Loop-Node-" + self.id());
        monitoringThread.start();   // start the thread

        // Start periodic MQTT telemetry thread
        node.getMqttManager().startMqttTelemetryPublishing();
    }

    /**
     * Retrieves a window from the buffer, computes the average, forwards it to managers,
     * and triggers calibration if the anomaly threshold is crossed.
     */
    private void processSensorWindow() {
        try {
            // Block until 8 measurements are ready (50% overlap step on subsequent reads)
            List<Measurement> window = buffer.readAllAndClear();
            if (window.isEmpty()) {
                return;
            }

            // Compute window average
            double average = computeAverage(window);
            
            // Track average value for criticality calculation
            node.getCoordinator().setLastCalculatedAverage(average);

            // Buffer the average for MQTT telemetry
            node.getMqttManager().addAverage(average);

            System.out.println("[PEER " + self.id() + "] [" + node.getState() + "] Calculated sliding window average: " 
                    + String.format("%.2f", average) + " (Threshold: " + vibrationThreshold + ")");

            // Check if threshold exceeded to trigger calibration transition
            if (average > vibrationThreshold && node.getState() == OperationalState.FULLY_OPERATIONAL) {
                node.transitionToWaitingForCalibration(average);
            }

        } catch (Exception e) {
            if (running) {
                System.err.println("[PEER " + self.id() + "] Error in sensor monitoring loop: " + e.getMessage());
            }
        }
    }

    /**
     * Stops the sensor simulator and shuts down the background monitoring thread.
     */
    public synchronized void stopMonitoring() {
        running = false;
        sensor.stopMeasuring();
        buffer.clear(); // Wake up wait() blocks inside SlidingWindowBuffer
        if (monitoringThread != null) {
            monitoringThread.interrupt();
            try {
                monitoringThread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            monitoringThread = null;
        }
        System.out.println("[PEER " + self.id() + "] Sensor monitoring loop stopped.");
    }
}
