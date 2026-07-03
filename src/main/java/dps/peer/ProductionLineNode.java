package dps.peer;

import dps.common.model.OperationalState;
import dps.common.model.ProductionLine;
import dps.peer.config.NodeConfig;
import dps.peer.net.NetworkManager;
import dps.peer.mqtt.MqttManager;
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

    // Ricart-Agrawala variables
    private long logicalClock = 0;
    private long requestTimestamp = 0;
    private double requestCriticality = 0.0;
    private final Map<Integer, StreamObserver<CalibrationReply>> deferredObservers = new HashMap<>();   // deferred response queue (pending gRPC StreamObserver)
    private double lastCalculatedAverage = 0.0;
    
    // Reply tracking by peer ID
    private final Set<Integer> repliesReceived = new HashSet<>();

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

    /**
     * Sends a calibration request to a single target peer asynchronously.
     * This is used both during initial broadcast and when yielding to a higher-priority peer.
     * By re-sending a request to the higher-priority peer we yielded to, we force that peer (which
     * is currently waiting or calibrating) to defer us and add us to its deferredObservers list,
     * ensuring it replies to us when it finishes calibrating. This prevents dynamic priority deadlocks.
     */
    public void sendCalibrationRequestToPeerAsynchronously(ProductionLine peer) {
        new Thread(() -> {
            sendCalibrationRequestToPeer(peer);
        }, "Re-Request-Thread-Node-" + self.id() + "-to-" + peer.id()).start();
    }

    /**
     * Executes the blocking gRPC call to request calibration from a single peer.
     */
    public void sendCalibrationRequestToPeer(ProductionLine peer) {
        double reqCriticality;
        long reqTimestamp;
        synchronized (this) {
            reqCriticality = this.requestCriticality;
            reqTimestamp = this.requestTimestamp;
        }

        ManagedChannel channel = null;
        try {
            channel = ManagedChannelBuilder.forAddress(peer.ip(), peer.port())
                    .usePlaintext()
                    .build();

            PeerServiceGrpc.PeerServiceBlockingStub stub = PeerServiceGrpc.newBlockingStub(channel);

            CalibrationRequest request = CalibrationRequest.newBuilder()
                    .setSenderId(self.id())
                    .setCriticality(reqCriticality)
                    .setTimestamp(reqTimestamp)
                    .build();

            /*
             * Send request with a 120-second (2-minute) timeout.
             * Why 120 seconds:
             * 1) Queue Accumulation: When multiple nodes (e.g., 15+) want to calibrate concurrently,
             *    their calibration times (up to 7 seconds each) accumulate in the queue. A 120-second
             *    deadline ensures healthy waiting nodes do not time out prematurely, preserving mutual exclusion.
             * 2) Crash Handling (Section 10.2): If a peer process crashes, the OS closes the TCP socket immediately.
             *    gRPC detects this instantly (throwing UNAVAILABLE in milliseconds), meaning we don't wait 120s
             *    to recover from a standard process crash.
             */
            stub.withDeadlineAfter(120, TimeUnit.SECONDS).requestCalibration(request);

            // Reply received successfully
            addReply(peer.id());
            System.out.println("[PEER " + self.id() + "] [" + getState() + "] Received CalibrationReply from Node " + peer.id());

        } catch (Exception e) {
            System.err.println("[PEER " + self.id() + "] [" + getState() + "] ❌ Failed to get CalibrationReply from Node " 
                    + peer.id() + " - Error: " + e.getMessage());
            // In case of communication failure or timeout, treat as implicit reply to avoid deadlocks
            addReply(peer.id());
        } finally {
            if (channel != null) {
                try {
                    channel.shutdown().awaitTermination(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public void requestCalibration() {
        List<ProductionLine> currentPeers = getPeers(); // synchronized

        synchronized (this) {
            incrementClock(); // send event -> increment Lamport's clock
            requestTimestamp = getLogicalClock(); // memorize current clock value
            requestCriticality = calcCriticality();  // calculate criticality based on threshold
            resetRepliesReceived(); // reset replies counter to start new round
        }

        if (currentPeers.isEmpty()) {
            System.out.println("[PEER " + self.id() + "] [" + getState() + "] No peers in topology. No replies needed.");
            return;
        }

        System.out.println("[PEER " + self.id() + "] [" + getState() + "] Requesting calibration from " + currentPeers.size() 
                + " peer(s) in parallel (Clock: " + requestTimestamp 
                + ", Criticality: " + String.format("%.4f", requestCriticality) + ")...");

        for (ProductionLine peer : currentPeers) {
            sendCalibrationRequestToPeerAsynchronously(peer);
        }
    }

    public double calcCriticality() {
        return (getLastCalculatedAverage() - vibrationThreshold) / vibrationThreshold;
    }

    /**
     * Blocks the current thread and requests calibration access from peers.
     * Transitions state to UNDER_CALIBRATION and performs the simulated calibration once allowed.
     */
    public void enterCalibrationAndWait() {

        // The method is not entirely synchronized, otherwise we would block all threads for the entire duration of the calibration or the network communications
        // so the synchronization is implemented at a finer granularity.

        double avg = getLastCalculatedAverage();
        
        System.out.println("[PEER " + self.id() + "] [WAITING_FOR_CALIBRATION] Initiating Ricart-Agrawala calibration sequence...");
        
        // 1. Broadcast the requests to peers
        requestCalibration();

        // 2. Wait until we receive all replies
        int requiredReplies = getPeerCount();
        synchronized (this) {
            while (getRepliesReceived() < requiredReplies) {
                try {
                    System.out.println("[PEER " + self.id() + "] [" + getState() + "] Waiting for replies... (Progress: " 
                            + getRepliesReceived() + "/" + requiredReplies + ")");
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            
            // 3. Enter calibration section
            setState(OperationalState.UNDER_CALIBRATION);
        }

        // Generate random calibration duration between 3 and 7 seconds
        long duration = 3000 + (long) (Math.random() * 4000);
        System.out.println("[PEER " + self.id() + "] [UNDER_CALIBRATION] 🛠️ Entered calibration mode. Calibrating for " + duration + " ms...");
        
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("[PEER " + self.id() + "] [UNDER_CALIBRATION] Calibration execution finished.");
    }

    /**
     * Releases the calibration resource, replies to all deferred peer requests,
     * updates the state to FULLY_OPERATIONAL, and restarts the physical sensor.
     */
    public void releaseCalibration() {
        List<StreamObserver<CalibrationReply>> observers;

        synchronized (this) {
            setState(OperationalState.FULLY_OPERATIONAL);
            observers = getAndClearDeferredObservers();     // get the observers accumulated when calibration was locked by this
        }

        System.out.println("[PEER " + self.id() + "] [FULLY_OPERATIONAL] Calibration completed. Releasing " 
                + observers.size() + " deferred replies...");

        for (StreamObserver<CalibrationReply> observer : observers) {       // iterate on the observers
            try {
                observer.onNext(CalibrationReply.getDefaultInstance());     // send consensus reply
                observer.onCompleted();     // close connection with peer (client)
            } catch (Exception e) {
                System.err.println("[PEER " + self.id() + "] [" + getState() + "] ❌ Failed to send deferred reply to peer - Error: " + e.getMessage());
            }
        }

        // Restart physical sensor measuring loop
        sensor.startMeasuring();
        System.out.println("[PEER " + self.id() + "] [" + getState() + "] Physical sensor simulator resumed.");
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

    // Thread-safe Lamport clock and RA helper methods
    public synchronized long getLogicalClock() {
        return logicalClock;
    }

    public synchronized void incrementClock() {
        logicalClock++;
    }

    public synchronized void updateClockOnReceive(long receivedTime) {
        logicalClock = Math.max(logicalClock, receivedTime) + 1;
    }

    public synchronized void addDeferredObserver(int peerId, StreamObserver<CalibrationReply> observer, String reason) {
        deferredObservers.put(peerId, observer);
        System.out.println("[PEER " + self.id() + "] [" + getState() + "] Deferring reply to Node " + peerId + " - Reason: " + reason);
    }

    public synchronized void addDeferredObserver(int peerId, StreamObserver<CalibrationReply> observer) {
        addDeferredObserver(peerId, observer, "none");
    }

    public synchronized List<StreamObserver<CalibrationReply>> getAndClearDeferredObservers() {
        List<StreamObserver<CalibrationReply>> observers = new ArrayList<>(deferredObservers.values());
        deferredObservers.clear();
        return observers;
    }

    public synchronized double getLastCalculatedAverage() {
        return lastCalculatedAverage;
    }

    public synchronized void setLastCalculatedAverage(double average) {
        this.lastCalculatedAverage = average;
    }

    // Reply tracking methods
    public synchronized void addReply(int peerId) {
        repliesReceived.add(peerId);
        notifyAll(); // Wake up thread waiting for replies
    }

    public synchronized void removeReply(int peerId) {
        repliesReceived.remove(peerId);
        notifyAll(); // Wake up thread in case count drops
    }

    public synchronized boolean hasReplyFrom(int peerId) {
        return repliesReceived.contains(peerId);
    }

    public synchronized void incrementRepliesReceived() {
        addReply(-repliesReceived.size() - 1); // backward compatibility helper
    }

    public synchronized int getRepliesReceived() {
        return repliesReceived.size();
    }

    public synchronized void resetRepliesReceived() {
        repliesReceived.clear();
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
