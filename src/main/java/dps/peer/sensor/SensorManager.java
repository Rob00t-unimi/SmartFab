package dps.peer.sensor;

import dps.common.model.OperationalState;
import dps.common.model.ProductionLine;
import dps.peer.ProductionLineNode;
import sensor.Measurement;
import sensor.MonitoringSensor;

import java.util.List;

/**
 * Manages the physical sensor simulator, the sliding window buffer, and the
 * consumer thread loop that calculates vibration averages.
 */
public class SensorManager {

    private final ProductionLineNode node;
    private final ProductionLine self;
    private final double vibrationThreshold;

    // Sensor and buffer instances
    private final SlidingWindowBuffer buffer = new SlidingWindowBuffer();
    private final MonitoringSensor sensor = new MonitoringSensor(buffer);

    private Thread monitoringThread;    // consumer thread
    private volatile boolean running = true;    // `volatile` saves to RAM, so all threads see exactly that value.

    public SensorManager(ProductionLineNode node, ProductionLine self, double vibrationThreshold) {
        if (node == null) {
            throw new IllegalArgumentException("ProductionLineNode reference cannot be null.");
        }
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

    public boolean isRunning() {
        return running;
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

        monitoringThread = new Thread(() -> {   // create a thread with this lambda
            while (running) {
                try {
                    // Block until 8 measurements are ready (50% overlap step on subsequent reads)
                    List<Measurement> window = buffer.readAllAndClear();
                    if (window.isEmpty()) {
                        continue;
                    }

                    // Compute window average
                    double sum = 0;
                    for (Measurement m : window) {
                        sum += m.value();
                    }
                    double average = sum / window.size();
                    
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
                    if (!running) {
                        break;
                    }
                    System.err.println("[PEER " + self.id() + "] Error in sensor monitoring loop: " + e.getMessage());
                }
            }
        });
        monitoringThread.setName("Sensor-Monitoring-Loop-Node-" + self.id());
        monitoringThread.start();   // start the thread

        // Start periodic MQTT telemetry thread
        node.getMqttManager().startMqttTelemetryPublishing();
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
}
