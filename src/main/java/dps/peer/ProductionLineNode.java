package dps.peer;

import dps.common.model.OperationalState;
import dps.common.model.ProductionLine;
import dps.peer.proto.CalibrationReply;
import dps.peer.proto.NodeIdentity;
import dps.peer.proto.PeerServiceGrpc;
import dps.peer.proto.PresentationRequest;
import dps.peer.proto.PresentationResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import sensor.Measurement;
import sensor.MonitoringSensor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ProductionLineNode {

    public record NodeConfig(ProductionLine self, String serverUrl) {}

    // Map of active peers, manually synchronized on 'this'
    private final Map<Integer, ProductionLine> peers = new HashMap<>();

    private final ProductionLine self;
    private final String serverUrl;
    
    private Server grpcServer;

    // Sensor and state properties (Lab 6)
    private final SlidingWindowBuffer buffer = new SlidingWindowBuffer();
    private final MonitoringSensor sensor = new MonitoringSensor(buffer);
    private OperationalState state = OperationalState.FULLY_OPERATIONAL;

    private Thread monitoringThread;
    private volatile boolean running = true;

    // Ricart-Agrawala variables (Lab 6 - Commit 2 & 3)
    private long logicalClock = 0;
    private final Map<Integer, StreamObserver<CalibrationReply>> deferredObservers = new HashMap<>();
    private double lastCalculatedAverage = 0.0;
    
    // Reply count tracking (Lab 6 - Feature 3 Commit 1)
    private int repliesReceived = 0;

    public ProductionLineNode(ProductionLine self, String serverUrl) {
        if (self == null) {
            throw new IllegalArgumentException("ProductionLine identity cannot be null.");
        }
        this.self = self;
        this.serverUrl = serverUrl;
    }

    public static void main(String[] args) {
        ProductionLineNode node = null;
        try {
            NodeConfig config = parseArgs(args);
            node = new ProductionLineNode(config.self(), config.serverUrl());

            System.out.println("[LINEA " + node.getSelf().id() + "] Starting node on " + node.getSelf().ip() + ":" + node.getSelf().port());
            System.out.println("[LINEA " + node.getSelf().id() + "] Admin Server URL: " + node.getServerUrl());

            // 1. Start gRPC Server first so that peers can reach us as soon as we register
            node.startGrpcServer();

            // 2. REST Registration
            node.registerWithAdminServer();

            // 3. gRPC Presentation to all registered peers in parallel
            node.presentSelfToPeers();

            // 4. Start monitoring sensor and window consumer loop
            node.startMonitoring();

            System.out.println("[LINEA " + node.getSelf().id() + "] Node is running. Press Ctrl+C to exit.");
            
            // Block until JVM is terminated
            node.blockUntilShutdown();

        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            System.err.println("Usage: java dps.peer.ProductionLineNode <id> <ip> <port> [serverUrl]");
            System.exit(1);
        } catch (IllegalStateException e) {
            System.err.println("Startup Failed: " + e.getMessage());
            if (node != null) {
                node.stopGrpcServer();
                node.stopMonitoring();
            }
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Unexpected Error: " + e.getMessage());
            if (node != null) {
                node.stopGrpcServer();
                node.stopMonitoring();
            }
            System.exit(1);
        }
    }

    public static NodeConfig parseArgs(String[] args) {
        if (args == null || args.length < 3) {
            throw new IllegalArgumentException("Insufficient arguments. ID, IP, and Port are required.");
        }

        int id;
        try {
            id = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("ID must be an integer.");
        }

        String ip = args[1];

        int port;
        try {
            port = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Port must be an integer.");
        }

        String serverUrl = args.length >= 4 ? args[3] : "http://localhost:8080";

        // This will perform the internal validations of the ProductionLine record (IP format, port range, etc.)
        ProductionLine self = new ProductionLine(id, ip, port);

        return new NodeConfig(self, serverUrl);
    }

    /**
     * Starts the local gRPC Server on the configured port.
     */
    public synchronized void startGrpcServer() throws IOException {
        if (grpcServer != null) {
            return;
        }

        grpcServer = ServerBuilder.forPort(self.port())
                .addService(new PeerServiceImpl(this))
                .build()
                .start();

        System.out.println("[LINEA " + self.id() + "] gRPC server started, listening on port " + self.port());

        // Add a shutdown hook to stop the gRPC server when JVM shuts down
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[LINEA " + self.id() + "] Shutdown hook triggered. Stopping gRPC server and monitoring...");
            ProductionLineNode.this.stopGrpcServer();
            ProductionLineNode.this.stopMonitoring();
        }));
    }

    /**
     * Gracefully stops the local gRPC Server.
     */
    public synchronized void stopGrpcServer() {
        if (grpcServer != null) {
            grpcServer.shutdown();
            try {
                if (!grpcServer.awaitTermination(3, TimeUnit.SECONDS)) {
                    grpcServer.shutdownNow();
                }
            } catch (InterruptedException e) {
                grpcServer.shutdownNow();
                Thread.currentThread().interrupt();
            }
            grpcServer = null;
            System.out.println("[LINEA " + self.id() + "] gRPC server stopped.");
        }
    }

    /**
     * Blocks the thread until the gRPC server is terminated.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (grpcServer != null) {
            grpcServer.awaitTermination();
        }
    }

    /**
     * Registers this node with the Admin Server via REST POST.
     * Populates the local peer list with the response on success.
     */
    public void registerWithAdminServer() {
        RestTemplate restTemplate = new RestTemplate();
        String url = this.serverUrl + "/production-lines";

        try {
            // Perform HTTP POST request. Spring will automatically serialize 'this.self' into JSON
            // and deserialize the response JSON array into an array of ProductionLine.
            ProductionLine[] response = restTemplate.postForObject(url, this.self, ProductionLine[].class);
            if (response != null) {
                for (ProductionLine peer : response) {
                    addPeer(peer);
                }
                System.out.println("[LINEA " + self.id() + "] REST registration successful. Loaded " + response.length + " peer(s) from Admin Server.");
            }
        } catch (HttpClientErrorException.Conflict e) {
            throw new IllegalStateException("Registration conflict: Node with ID " + self.id() + " is already registered.");
        } catch (ResourceAccessException e) {
            throw new IllegalStateException("Connection failed: Admin Server is offline or unreachable at " + url);
        } catch (Exception e) {
            throw new IllegalStateException("Registration failed due to unexpected error: " + e.getMessage(), e);
        }
    }

    /**
     * Broadcasts a gRPC presentation request in parallel to all currently known peers.
     */
    public void presentSelfToPeers() {
        List<ProductionLine> currentPeers = getPeers();
        if (currentPeers.isEmpty()) {
            System.out.println("[LINEA " + self.id() + "] No existing peers to present to in local network view.");
            return;
        }

        System.out.println("[LINEA " + self.id() + "] Sending gRPC presentation requests to " + currentPeers.size() + " peer(s) in parallel...");

        // P2P broadcasts must be done in parallel. We use CachedThreadPool.
        ExecutorService executor = Executors.newCachedThreadPool();

        for (ProductionLine peer : currentPeers) {
            executor.submit(() -> {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(peer.ip(), peer.port())
                            .usePlaintext()
                            .build();

                    PeerServiceGrpc.PeerServiceBlockingStub stub = PeerServiceGrpc.newBlockingStub(channel);

                    NodeIdentity identity = NodeIdentity.newBuilder()
                            .setId(self.id())
                            .setIp(self.ip())
                            .setPort(self.port())
                            .build();

                    PresentationRequest request = PresentationRequest.newBuilder()
                            .setSender(identity)
                            .build();

                    // Timeout of 3 seconds for presentation response
                    PresentationResponse response = stub.withDeadlineAfter(3, TimeUnit.SECONDS).present(request);
                    if (response.getAccepted()) {
                        System.out.println("[LINEA " + self.id() + "] gRPC presentation ACCEPTED by Node " + peer.id());
                    } else {
                        System.out.println("[LINEA " + self.id() + "] ⚠️ gRPC presentation REJECTED by Node " + peer.id());
                    }

                } catch (Exception e) {
                    System.err.println("[LINEA " + self.id() + "] ❌ gRPC presentation FAILED to Node " + peer.id() + " - Error: " + e.getMessage());
                } finally {
                    if (channel != null) {
                        try {
                            channel.shutdown().awaitTermination(1, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            });
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Starts the physical sensor simulator and the background thread that consumes
     * measurements from the sliding window buffer.
     */
    public synchronized void startMonitoring() {
        if (monitoringThread != null) {
            return;
        }

        running = true;
        sensor.startMeasuring();
        System.out.println("[LINEA " + self.id() + "] Physical sensor simulator started.");

        monitoringThread = new Thread(() -> {
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
                    
                    // Track average value for criticality calculation (Lab 6)
                    setLastCalculatedAverage(average);

                    System.out.println("[LINEA " + self.id() + "] [" + getState() + "] Calculated sliding window average: " 
                            + String.format("%.2f", average) + " (Soglia: 80.0)");

                    // Check if threshold exceeded to trigger calibration transition
                    if (average > 80.0 && getState() == OperationalState.FULLY_OPERATIONAL) {
                        transitionToWaitingForCalibration(average);
                    }

                } catch (Exception e) {
                    if (!running) {
                        break;
                    }
                    System.err.println("[LINEA " + self.id() + "] Error in sensor monitoring loop: " + e.getMessage());
                }
            }
        });
        monitoringThread.setName("Sensor-Monitoring-Loop-Node-" + self.id());
        monitoringThread.start();
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
        System.out.println("[LINEA " + self.id() + "] Sensor monitoring loop stopped.");
    }

    /**
     * Local state transition to WAITING_FOR_CALIBRATION.
     * Pauses the physical sensor simulator and clears the window buffer.
     */
    private synchronized void transitionToWaitingForCalibration(double average) {
        setState(OperationalState.WAITING_FOR_CALIBRATION);

        System.out.println("[LINEA " + self.id() + "] [FULLY_OPERATIONAL -> WAITING_FOR_CALIBRATION] ⚠️ Average vibration " 
                + String.format("%.2f", average) + " exceeded threshold 80.0! Pausing sensor, clearing buffer, and requesting calibration...");

        sensor.pauseMeasuring();
        buffer.clear();

        // TODO: In Feature 3, we will trigger the Ricart-Agrawala calibration request here
    }

    /**
     * Sends a gRPC request for calibration to all active peers in parallel.
     */
    public void requestCalibration(double averageVibration) {
        List<ProductionLine> currentPeers = getPeers();

        long requestTimestamp;
        double requestCriticality;

        synchronized (this) {
            incrementClock();
            requestTimestamp = getLogicalClock();
            requestCriticality = (averageVibration - 80.0) / 80.0;
            resetRepliesReceived();
        }

        if (currentPeers.isEmpty()) {
            System.out.println("[LINEA " + self.id() + "] No peers in topology. No replies needed.");
            return;
        }

        System.out.println("[LINEA " + self.id() + "] Requesting calibration from " + currentPeers.size() 
                + " peer(s) in parallel (Clock: " + requestTimestamp 
                + ", Criticality: " + String.format("%.4f", requestCriticality) + ")...");

        ExecutorService executor = Executors.newCachedThreadPool();

        for (ProductionLine peer : currentPeers) {
            executor.submit(() -> {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(peer.ip(), peer.port())
                            .usePlaintext()
                            .build();

                    PeerServiceGrpc.PeerServiceBlockingStub stub = PeerServiceGrpc.newBlockingStub(channel);

                    dps.peer.proto.CalibrationRequest request = dps.peer.proto.CalibrationRequest.newBuilder()
                            .setSenderId(self.id())
                            .setCriticality(requestCriticality)
                            .setTimestamp(requestTimestamp)
                            .build();

                    // Timeout of 5 seconds for reply response
                    stub.withDeadlineAfter(5, TimeUnit.SECONDS).requestCalibration(request);

                    // Reply received successfully
                    incrementRepliesReceived();
                    System.out.println("[LINEA " + self.id() + "] Received CalibrationReply from Node " + peer.id());

                } catch (Exception e) {
                    System.err.println("[LINEA " + self.id() + "] ❌ Failed to get CalibrationReply from Node " 
                            + peer.id() + " - Error: " + e.getMessage());
                    // In case of communication failure or timeout, treat as implicit reply to avoid deadlocks
                    incrementRepliesReceived();
                } finally {
                    if (channel != null) {
                        try {
                            channel.shutdown().awaitTermination(1, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            });
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(6, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public synchronized OperationalState getState() {
        return state;
    }

    public synchronized void setState(OperationalState state) {
        this.state = state;
    }

    public SlidingWindowBuffer getBuffer() {
        return buffer;
    }

    public MonitoringSensor getSensor() {
        return sensor;
    }

    // Thread-safe Lamport clock and RA helper methods (Lab 6 - Commit 2 & 3)
    public synchronized long getLogicalClock() {
        return logicalClock;
    }

    public synchronized void incrementClock() {
        logicalClock++;
    }

    public synchronized void updateClockOnReceive(long receivedTime) {
        logicalClock = Math.max(logicalClock, receivedTime) + 1;
    }

    public synchronized void addDeferredObserver(int peerId, StreamObserver<CalibrationReply> observer) {
        deferredObservers.put(peerId, observer);
        System.out.println("[LINEA " + self.id() + "] Deferring reply to Node " + peerId);
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

    // Reply tracking methods (Lab 6 - Feature 3 Commit 1)
    public synchronized void incrementRepliesReceived() {
        repliesReceived++;
        notifyAll(); // Wake up thread waiting for replies
    }

    public synchronized int getRepliesReceived() {
        return repliesReceived;
    }

    public synchronized void resetRepliesReceived() {
        repliesReceived = 0;
    }

    public ProductionLine getSelf() {
        return self;
    }

    public String getServerUrl() {
        return serverUrl;
    }

    // Thread-safe peer management using basic synchronized blocks
    public synchronized void addPeer(ProductionLine peer) {
        if (peer == null) {
            throw new IllegalArgumentException("Peer cannot be null.");
        }
        peers.put(peer.id(), peer);
    }

    public synchronized void removePeer(int peerId) {
        peers.remove(peerId);
    }

    public synchronized List<ProductionLine> getPeers() {
        return new ArrayList<>(peers.values());
    }

    public synchronized int getPeerCount() {
        return peers.size();
    }
}
