package dps.peer;

import dps.common.model.OperationalState;
import dps.common.model.ProductionLine;
import dps.peer.config.NodeConfig;
import dps.peer.net.NetworkManager;
import dps.peer.mqtt.MqttManager;
import dps.peer.coordinator.RicartAgrawalaCoordinator;
import dps.peer.proto.CalibrationReply;
import dps.peer.proto.NodeIdentity;
import dps.peer.proto.PeerServiceGrpc;
import dps.peer.proto.PresentationRequest;
import dps.peer.proto.PresentationResponse;
import dps.peer.proto.CalibrationRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import sensor.Measurement;
import sensor.MonitoringSensor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ProductionLineNode {

    private final ProductionLine self;
    private final String serverUrl;
    
    private final NetworkManager networkManager;

    // Sensor and state properties
    private final SlidingWindowBuffer buffer = new SlidingWindowBuffer();
    private final MonitoringSensor sensor = new MonitoringSensor(buffer);
    private OperationalState state = OperationalState.FULLY_OPERATIONAL;
    private final double vibrationThreshold = 80.0;

    private Thread monitoringThread;    // consumer thread
    private volatile boolean running = true;    // `volatile` saves to RAM, so all threads see exactly that value.

    private final RicartAgrawalaCoordinator coordinator;

    private final String mqttBrokerUrl;
    private final MqttManager mqttManager;

    public ProductionLineNode(ProductionLine self, String serverUrl) {
        this(self, serverUrl, "tcp://localhost:1883");
    }

    public ProductionLineNode(ProductionLine self, String serverUrl, String mqttBrokerUrl) {
        if (self == null) {
            throw new IllegalArgumentException("ProductionLine identity cannot be null.");
        }
        this.self = self;
        this.serverUrl = serverUrl;
        this.mqttBrokerUrl = mqttBrokerUrl;
        this.networkManager = new NetworkManager(this, self, serverUrl);
        this.mqttManager = new MqttManager(this, self, mqttBrokerUrl);
        this.coordinator = new RicartAgrawalaCoordinator(this, self);
    }

    public RicartAgrawalaCoordinator getCoordinator() {
        return coordinator;
    }

    public MqttManager getMqttManager() {
        return mqttManager;
    }

    public boolean isRunning() {
        return running;
    }

    public NetworkManager getNetworkManager() {
        return networkManager;
    }

    public static void main(String[] args) {
        ProductionLineNode node = null;
        try {
            NodeConfig config = NodeConfig.parseArgs(args);
            // Redirect console output to LogUtils for clean colorized and shared logs
            dps.common.util.LogUtils.redirectSystemOutAndErr("PEER-" + config.self().id(), dps.common.util.LogUtils.getPeerColor(String.valueOf(config.self().id())));
            node = new ProductionLineNode(config.self(), config.serverUrl(), config.mqttBrokerUrl());

            System.out.println("[PEER " + node.getSelf().id() + "] Starting node on " + node.getSelf().ip() + ":" + node.getSelf().port());
            System.out.println("[PEER " + node.getSelf().id() + "] Admin Server URL: " + node.getServerUrl());

            // 1. Start gRPC Server first so that peers can reach us as soon as we register
            node.getNetworkManager().startGrpcServer();

            // 2. REST Registration
            node.getNetworkManager().registerWithAdminServer();

            // 3. gRPC Presentation to all registered peers in parallel
            node.getNetworkManager().presentSelfToPeers();

            // 4. Connect to MQTT Broker
            node.connectMqtt();

            // 5. Start Monitoring Sensor and Processing Thread Loops
            node.startMonitoring();

            System.out.println("[PEER " + node.getSelf().id() + "] Node is running. Press Ctrl+C to exit.");
            
            // Block until JVM is terminated
            node.getNetworkManager().blockUntilGRPCShutdown();

        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            System.err.println("Usage: java dps.peer.ProductionLineNode <id> <ip> <port> [serverUrl]");
            System.exit(1);
        } catch (IllegalStateException e) {
            System.err.println("Startup Failed: " + e.getMessage());
            if (node != null) {
                node.getNetworkManager().stopGrpcServer();
                node.stopMonitoring();
            }
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Unexpected Error: " + e.getMessage());
            if (node != null) {
                node.getNetworkManager().stopGrpcServer();
                node.stopMonitoring();
            }
            System.exit(1);
        }
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
                    setLastCalculatedAverage(average);

                    // Buffer the average for MQTT telemetry
                    mqttManager.addAverage(average);

                    System.out.println("[PEER " + self.id() + "] [" + getState() + "] Calculated sliding window average: " 
                            + String.format("%.2f", average) + " (Threshold: " + vibrationThreshold + ")");

                    // Check if threshold exceeded to trigger calibration transition
                    if (average > vibrationThreshold && getState() == OperationalState.FULLY_OPERATIONAL) {
                        transitionToWaitingForCalibration(average);
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
        mqttManager.startMqttTelemetryPublishing();
    }

    /**
     * Stops the sensor simulator and shuts down the background monitoring thread and mqtt publisher.
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

        // Stop MQTT publisher and disconnect Paho client
        mqttManager.stop();
        System.out.println("[PEER " + self.id() + "] Sensor monitoring loop stopped.");
    }

    // Thread-safe MQTT delegation to MqttManager
    public synchronized void connectMqtt() {
        mqttManager.connectMqtt();
    }

    public synchronized void disconnectMqtt() {
        mqttManager.disconnectMqtt();
    }

    public void publishTelemetry(String payload) {
        mqttManager.publishTelemetry(payload);
    }

    /**
     * Local state transition to WAITING_FOR_CALIBRATION.
     * Pauses the physical sensor simulator and clears the window buffer.
     */
    private synchronized void transitionToWaitingForCalibration(double average) {
        setState(OperationalState.WAITING_FOR_CALIBRATION);

        System.out.println("[PEER " + self.id() + "] [WAITING_FOR_CALIBRATION] ⚠️ Average vibration " 
                + String.format("%.2f", average) + " exceeded threshold " + vibrationThreshold + "! Pausing sensor, clearing buffer, and requesting calibration...");

        sensor.pauseMeasuring();
        buffer.clear();

        // Asynchronously start the calibration sequence in a separate thread
        new Thread(() -> {
            enterCalibrationAndWait();
            releaseCalibration();
        }, "Calibration-Coordinator-Thread-Node-" + self.id()).start();
    }

    /**
     * Ricart-Agrawala client: increments the Lamport clock, calculates criticality based on the threshold being exceeded, and resets the response counter.
     * Sends a gRPC request for calibration to all active peers in parallel.
     */
    public synchronized ProductionLine getPeerById(int id) {
        return networkManager.getPeerById(id);
    }

    // Ricart-Agrawala Coordinator Delegation Methods
    public void sendCalibrationRequestToPeerAsynchronously(ProductionLine peer) {
        coordinator.sendCalibrationRequestToPeerAsynchronously(peer);
    }

    public void sendCalibrationRequestToPeer(ProductionLine peer) {
        coordinator.sendCalibrationRequestToPeer(peer);
    }

    public void requestCalibration() {
        coordinator.requestCalibration();
    }

    public double calcCriticality() {
        return (getLastCalculatedAverage() - vibrationThreshold) / vibrationThreshold;
    }

    public void enterCalibrationAndWait() {
        coordinator.enterCalibrationAndWait();
    }

    public void releaseCalibration() {
        coordinator.releaseCalibration();
    }

    public synchronized OperationalState getState() {
        return state;
    }

    public synchronized void setState(OperationalState state) {
        OperationalState oldState = this.state;
        this.state = state;
        if (oldState != state) {
            mqttManager.publishStateUpdate(state);
        }
    }

    public void publishStatus(String payload) {
        mqttManager.publishStatus(payload);
    }

    public SlidingWindowBuffer getBuffer() {
        return buffer;
    }

    public MonitoringSensor getSensor() {
        return sensor;
    }

    // Thread-safe Lamport clock and RA helper methods delegated to coordinator
    public synchronized long getLogicalClock() {
        return coordinator.getLogicalClock();
    }

    public synchronized void incrementClock() {
        coordinator.incrementClock();
    }

    public synchronized void updateClockOnReceive(long receivedTime) {
        coordinator.updateClockOnReceive(receivedTime);
    }

    public synchronized void addDeferredObserver(int peerId, StreamObserver<CalibrationReply> observer, String reason) {
        coordinator.addDeferredObserver(peerId, observer, reason);
    }

    public synchronized void addDeferredObserver(int peerId, StreamObserver<CalibrationReply> observer) {
        coordinator.addDeferredObserver(peerId, observer);
    }

    public synchronized List<StreamObserver<CalibrationReply>> getAndClearDeferredObservers() {
        return coordinator.getAndClearDeferredObservers();
    }

    public synchronized double getLastCalculatedAverage() {
        return coordinator.getLastCalculatedAverage();
    }

    public synchronized void setLastCalculatedAverage(double average) {
        coordinator.setLastCalculatedAverage(average);
    }

    // Reply tracking methods delegated to coordinator
    public synchronized void addReply(int peerId) {
        coordinator.addReply(peerId);
    }

    public synchronized void removeReply(int peerId) {
        coordinator.removeReply(peerId);
    }

    public synchronized boolean hasReplyFrom(int peerId) {
        return coordinator.hasReplyFrom(peerId);
    }

    public synchronized void incrementRepliesReceived() {
        coordinator.incrementRepliesReceived();
    }

    public synchronized int getRepliesReceived() {
        return coordinator.getRepliesReceived();
    }

    public synchronized void resetRepliesReceived() {
        coordinator.resetRepliesReceived();
    }

    public ProductionLine getSelf() {
        return self;
    }

    public double getVibrationThreshold() {
        return vibrationThreshold;
    }

    void addAverageToBufferForTesting(double avg) {
        mqttManager.addAverage(avg);
    }

    List<Double> getLocalAveragesBufferSnapshot() {
        return mqttManager.getLocalAveragesBufferSnapshot();
    }

    public String getServerUrl() {
        return serverUrl;
    }

    // Thread-safe network delegation to NetworkManager
    public synchronized void startGrpcServer() throws java.io.IOException {
        networkManager.startGrpcServer();
    }

    public synchronized void stopGrpcServer() {
        networkManager.stopGrpcServer();
    }

    public void blockUntilGRPCShutdown() throws InterruptedException {
        networkManager.blockUntilGRPCShutdown();
    }

    public void registerWithAdminServer() {
        networkManager.registerWithAdminServer();
    }

    public void presentSelfToPeers() {
        networkManager.presentSelfToPeers();
    }

    // Thread-safe peer management delegated to NetworkManager
    public synchronized void addPeer(ProductionLine peer) {
        networkManager.addPeer(peer);
    }

    public synchronized void removePeer(int peerId) {
        networkManager.removePeer(peerId);
    }

    public synchronized List<ProductionLine> getPeers() {
        return networkManager.getPeers();
    }

    public synchronized int getPeerCount() {
        return networkManager.getPeerCount();
    }
}
