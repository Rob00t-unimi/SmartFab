package dps.peer;

import dps.common.model.OperationalState;
import dps.common.model.ProductionLine;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ProductionLineNode {

    public record NodeConfig(ProductionLine self, String serverUrl, String mqttBrokerUrl) {}

    // Map of active peers, manually synchronized on 'this'
    private final Map<Integer, ProductionLine> peers = new HashMap<>();

    private final ProductionLine self;
    private final String serverUrl;
    
    private Server grpcServer;

    // Sensor and state properties
    private final SlidingWindowBuffer buffer = new SlidingWindowBuffer();
    private final MonitoringSensor sensor = new MonitoringSensor(buffer);
    private OperationalState state = OperationalState.FULLY_OPERATIONAL;
    private final double vibrationThreshold = 80.0;

    private Thread monitoringThread;    // consumer thread
    private volatile boolean running = true;    // `volatile` saves to RAM, so all threads see exactly that value.

    // Ricart-Agrawala variables
    private long logicalClock = 0;
    private final Map<Integer, StreamObserver<CalibrationReply>> deferredObservers = new HashMap<>();   // deferred response queue (pending gRPC StreamObserver)
    private double lastCalculatedAverage = 0.0;
    
    // Reply count tracking
    private int repliesReceived = 0;

    // MQTT client configuration
    private MqttClient mqttClient;
    private final String mqttBrokerUrl;

    // MQTT buffer and publisher thread
    private final List<Double> localAveragesBuffer = new ArrayList<>();     // local averages from sensor to send via mqtt
    private Thread mqttPublisherThread; // 10 seconds cycle mqtt thread

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
    }

    public static void main(String[] args) {
        ProductionLineNode node = null;
        try {
            NodeConfig config = parseArgs(args);
            // Redirect console output to LogUtils for clean colorized and shared logs
            dps.common.util.LogUtils.redirectSystemOutAndErr("PEER-" + config.self().id(), dps.common.util.LogUtils.ANSI_GREEN);
            node = new ProductionLineNode(config.self(), config.serverUrl(), config.mqttBrokerUrl());

            System.out.println("[PEER " + node.getSelf().id() + "] Starting node on " + node.getSelf().ip() + ":" + node.getSelf().port());
            System.out.println("[PEER " + node.getSelf().id() + "] Admin Server URL: " + node.getServerUrl());

            // 1. Start gRPC Server first so that peers can reach us as soon as we register
            node.startGrpcServer();

            // 2. REST Registration
            node.registerWithAdminServer();

            // 3. gRPC Presentation to all registered peers in parallel
            node.presentSelfToPeers();

            // 4. Connect to MQTT Broker
            node.connectMqtt();

            // 5. Start monitoring sensor and window consumer loop
            node.startMonitoring();

            System.out.println("[PEER " + node.getSelf().id() + "] Node is running. Press Ctrl+C to exit.");
            
            // Block until JVM is terminated
            node.blockUntilGRPCShutdown();

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
        String mqttBrokerUrl = args.length >= 5 ? args[4] : "tcp://localhost:1883";

        // This will perform the internal validations of the ProductionLine record (IP format, port range, etc.)
        ProductionLine self = new ProductionLine(id, ip, port);

        return new NodeConfig(self, serverUrl, mqttBrokerUrl);
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

        System.out.println("[PEER " + self.id() + "] gRPC server started, listening on port " + self.port());

        // Add a shutdown hook to stop the gRPC server when JVM shuts down
        // starts a new thread that performs cleanup during the shutdown phase
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[PEER " + self.id() + "] Shutdown hook triggered. Stopping gRPC server and monitoring...");
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
                // If an interrupt occurs during the "try" phase, an exception is thrown and the interrupt must be re-raised.
                grpcServer.shutdownNow();
                Thread.currentThread().interrupt();
            }
            grpcServer = null;
            System.out.println("[PEER " + self.id() + "] gRPC server stopped.");
        }
    }

    /**
     * Blocks the thread until the gRPC server is terminated.
     */
    public void blockUntilGRPCShutdown() throws InterruptedException {
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
            // Perform HTTP POST request (the only post avaiable in /production-lines is "register").
            // Spring will automatically serialize 'this.self' into JSON
            // and deserialize the response JSON array into an array of ProductionLine.
            ProductionLine[] response = restTemplate.postForObject(url, this.self, ProductionLine[].class); // The third parameter tells Spring which object to deserialize the response into.
            if (response != null) {
                for (ProductionLine peer : response) {
                    addPeer(peer); // synchronized
                }
                System.out.println("[PEER " + self.id() + "] REST registration successful. Loaded " + response.length + " peer(s) from Admin Server.");
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
        List<ProductionLine> currentPeers = getPeers(); // synchronized
        if (currentPeers.isEmpty()) {
            System.out.println("[PEER " + self.id() + "] No existing peers to present to in local network view.");
            return;
        }

        System.out.println("[PEER " + self.id() + "] Sending gRPC presentation requests to " + currentPeers.size() + " peer(s) in parallel...");

        // P2P broadcasts must be done in parallel by using CachedThreadPool.
        ExecutorService executor = Executors.newCachedThreadPool();

        for (ProductionLine peer : currentPeers) {
            executor.submit(() -> {  // For each peer, we assign and execute a lambda to a thread into the pool (
                // Tasks are assigned to the pool threads sequentially within the loop, but the threads execute the gRPC calls in parallel, without waiting for the previous thread to establish or complete its connection.
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(peer.ip(), peer.port())  // gRPC channel creation
                            .usePlaintext() // http instead of https
                            .build();

                    // Creating a stub object locally allows server remote procedure calls to be made from it.
                    PeerServiceGrpc.PeerServiceBlockingStub stub = PeerServiceGrpc.newBlockingStub(channel); // BlockingStub, because the current thread waits for the RPC response.

                    // Constructing Messages using the Builder Pattern auto-generated by Protobuf
                    NodeIdentity identity = NodeIdentity.newBuilder()   // NodeIdentity (object generated by Protobuf)
                            .setId(self.id())
                            .setIp(self.ip())
                            .setPort(self.port())
                            .build();

                    // Construction of the request
                    PresentationRequest request = PresentationRequest.newBuilder()  // PresentationRequest (object generated by Protobuf)
                            .setSender(identity)
                            .build();

                    // Executing the gRPC call with a 3-second timeout to a gRPC server of another peer
                    PresentationResponse response = stub.withDeadlineAfter(3, TimeUnit.SECONDS).present(request);
                    if (response.getAccepted()) {
                        System.out.println("[PEER " + self.id() + "] [" + getState() + "] gRPC presentation ACCEPTED by Node " + peer.id());
                    } else {
                        System.out.println("[PEER " + self.id() + "] [" + getState() + "] ⚠️ gRPC presentation REJECTED by Node " + peer.id());
                    }

                } catch (Exception e) {
                    System.err.println("[PEER " + self.id() + "] [" + getState() + "] ❌ gRPC presentation FAILED to Node " + peer.id() + " - Error: " + e.getMessage());
                } finally {
                    if (channel != null) {
                        try {
                            // close the gRPC channel
                            channel.shutdown().awaitTermination(1, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            });
        }

        // Shutdown Thread Pool
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt(); // If an interrupt arrives that sends execution to the "catch" block, I re-throw it.
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
                    synchronized (localAveragesBuffer) {
                        localAveragesBuffer.add(average);
                    }

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
        startMqttTelemetryPublishing();
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

        // The MQTT publishing thread is safely stopped via interruption
        // waiting up to 2 seconds for it to shut down
        // and the client's TCP socket is closed.
        if (mqttPublisherThread != null) {
            mqttPublisherThread.interrupt();
            try {
                mqttPublisherThread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            mqttPublisherThread = null;
        }
        disconnectMqtt(); // Disconnect MQTT client
        System.out.println("[PEER " + self.id() + "] Sensor monitoring loop stopped.");
    }

    /**
     * Connects to the MQTT Broker
     */
    public synchronized void connectMqtt() {
        try {
            String clientId = "smartfab-peer-" + self.id();
            // instantiate client mqtt with eclipse paho.
            mqttClient = new MqttClient(mqttBrokerUrl, clientId, new MemoryPersistence());  // memory persistence keeps temporary messages not yet sent in RAM instead of in the FS
            MqttConnectOptions connOpts = new MqttConnectOptions();  // config connection parameters
            connOpts.setCleanSession(true);
            
            System.out.println("[PEER " + self.id() + "] [" + getState() + "] Connecting to MQTT Broker: " + mqttBrokerUrl + "...");
            mqttClient.connect(connOpts);   // open connection (wait broker response)
            System.out.println("[PEER " + self.id() + "] [" + getState() + "] Connected to MQTT Broker successfully.");
        } catch (Exception e) {
            throw new IllegalStateException("MQTT connection failed to broker " + mqttBrokerUrl + ": " + e.getMessage(), e);
        }
    }

    /**
     * Disconnects from the MQTT Broker
     */
    public synchronized void disconnectMqtt() {
        if (mqttClient != null && mqttClient.isConnected()) {
            try {
                System.out.println("[PEER " + self.id() + "] [" + getState() + "] Disconnecting from MQTT Broker...");
                mqttClient.disconnect();
                mqttClient.close();
                System.out.println("[PEER " + self.id() + "] [" + getState() + "] Disconnected from MQTT Broker successfully.");
            } catch (Exception e) {
                System.err.println("[PEER " + self.id() + "] ❌ Error disconnecting from MQTT Broker: " + e.getMessage());
            }
        }
    }

    /**
     * Publishes telemetry payload to the MQTT Broker.
     */
    public void publishTelemetry(String payload) {
        synchronized (this) {
            if (mqttClient == null || !mqttClient.isConnected()) {
                System.err.println("[PEER " + self.id() + "] Cannot publish telemetry: MQTT client not connected.");
                return;
            }
        }
        try {
            String topic = "smartfab/production-line/" + self.id() + "/telemetry";
            MqttMessage message = new MqttMessage(payload.getBytes());
            message.setQos(1);  // guarantees that the message is received at least once
            mqttClient.publish(topic, message);
        } catch (Exception e) {
            System.err.println("[PEER " + self.id() + "] ❌ Error publishing MQTT telemetry: " + e.getMessage());
        }
    }

    /**
     * Starts the periodic thread that publishes collected averages every 10 seconds.
     */
    private void startMqttTelemetryPublishing() {
        mqttPublisherThread = new Thread(() -> {
            while (running) {
                try {
                    Thread.sleep(10000); // Wait 10 seconds
                    
                    // Extract and clear the local averages buffer thread-safely
                    List<Double> averagesToPublish;
                    synchronized (localAveragesBuffer) {
                        averagesToPublish = new ArrayList<>(localAveragesBuffer);
                        localAveragesBuffer.clear();
                    }
                    
                    // Construct JSON payload using Jackson ObjectMapper
                    ObjectMapper mapper = new ObjectMapper();
                    ObjectNode rootNode = mapper.createObjectNode();
                    rootNode.put("id", self.id());
                    
                    ArrayNode averagesArray = mapper.createArrayNode();
                    for (double avg : averagesToPublish) {
                        averagesArray.add(avg);
                    }
                    rootNode.set("averages", averagesArray);
                    rootNode.put("timestamp", System.currentTimeMillis());
                    rootNode.put("state", getState().name());
                    
                    String payload = mapper.writeValueAsString(rootNode);

                    publishTelemetry(payload); // publish
                    
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    System.err.println("[PEER " + self.id() + "] Error in MQTT telemetry publisher: " + e.getMessage());
                }
            }
        });
        mqttPublisherThread.setName("MQTT-Telemetry-Publisher-Node-" + self.id());
        mqttPublisherThread.start();
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
    public void requestCalibration() {
        List<ProductionLine> currentPeers = getPeers(); // synchronized

        long requestTimestamp;
        double requestCriticality;

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

        ExecutorService executor = Executors.newCachedThreadPool();     // crate thread pool to send gRPC requests in parallel

        for (ProductionLine peer : currentPeers) {
            executor.submit(() -> {     // for each peer create a thread and start the task assigned without waiting other threads
                ManagedChannel channel = null;
                try {
                    // create gRPC channel
                    channel = ManagedChannelBuilder.forAddress(peer.ip(), peer.port())
                            .usePlaintext()
                            .build();

                    // create stub gRPC
                    PeerServiceGrpc.PeerServiceBlockingStub stub = PeerServiceGrpc.newBlockingStub(channel);

                    // construct message
                    CalibrationRequest request = CalibrationRequest.newBuilder()
                            .setSenderId(self.id())
                            .setCriticality(requestCriticality)
                            .setTimestamp(requestTimestamp)
                            .build();

                    /*
                     * Send request with a 60-second timeout.
                     * Why 60 seconds:
                     * 1) Queue Accumulation: When multiple nodes (e.g. 5) want to calibrate concurrently,
                     *    their calibration times (up to 7 seconds each) accumulate in the queue. A 60-second
                     *    deadline ensures healthy waiting nodes do not time out prematurely, preserving mutual exclusion.
                     * 2) Crash Handling (Section 10.2): If a peer process crashes, the OS closes the TCP socket immediately.
                     *    gRPC detects this instantly (throwing UNAVAILABLE in milliseconds), meaning we don't wait 60s
                     *    to recover from a standard process crash. The 60s deadline is just a backup for silent hangs.
                     */
                    stub.withDeadlineAfter(60, TimeUnit.SECONDS).requestCalibration(request);

                    // Reply received successfully
                    incrementRepliesReceived(); // increment replies counter
                    System.out.println("[PEER " + self.id() + "] [" + getState() + "] Received CalibrationReply from Node " + peer.id());

                } catch (Exception e) {
                    System.err.println("[PEER " + self.id() + "] [" + getState() + "] ❌ Failed to get CalibrationReply from Node " 
                            + peer.id() + " - Error: " + e.getMessage());
                    // In case of communication failure or timeout, treat as implicit reply to avoid deadlocks
                    incrementRepliesReceived();
                } finally {
                    if (channel != null) {
                        try {
                            // close gRPC channel
                            channel.shutdown().awaitTermination(1, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            });
        }

        // Shutdown the executor so threads terminate when tasks finish, but return immediately
        // to avoid blocking the calling thread.
        executor.shutdown();
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
            publishStateUpdate(state);
        }
    }

    /**
     * Publishes status payload to the MQTT Broker.
     */
    public void publishStatus(String payload) {
        synchronized (this) {
            if (mqttClient == null || !mqttClient.isConnected()) {
                System.err.println("[PEER " + self.id() + "] Cannot publish status: MQTT client not connected.");
                return;
            }
        }
        try {
            String topic = "smartfab/production-line/" + self.id() + "/status";
            MqttMessage message = new MqttMessage(payload.getBytes());
            message.setQos(1);  // at least 1 message
            mqttClient.publish(topic, message);
        } catch (Exception e) {
            System.err.println("[PEER " + self.id() + "] ❌ Error publishing MQTT status: " + e.getMessage());
        }
    }

    /**
     * Builds and asynchronously publishes state change update.
     */
    private void publishStateUpdate(OperationalState newState) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode rootNode = mapper.createObjectNode();
            rootNode.put("id", self.id());
            rootNode.put("state", newState.name());
            rootNode.put("timestamp", System.currentTimeMillis());
            
            if (newState == OperationalState.WAITING_FOR_CALIBRATION || newState == OperationalState.UNDER_CALIBRATION) {
                rootNode.put("criticality", calcCriticality());
            }
            
            String payload = mapper.writeValueAsString(rootNode);
            
            // Asynchronously publish to avoid holding the monitor lock of 'this' during network I/O
            new Thread(() -> {
                publishStatus(payload);
            }, "MQTT-Status-Publisher-Node-" + self.id()).start();
            
        } catch (Exception e) {
            System.err.println("[PEER " + self.id() + "] Error building MQTT status payload: " + e.getMessage());
        }
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

    public double getVibrationThreshold() {
        return vibrationThreshold;
    }

    void addAverageToBufferForTesting(double avg) {
        synchronized (localAveragesBuffer) {
            localAveragesBuffer.add(avg);
        }
    }

    List<Double> getLocalAveragesBufferSnapshot() {
        synchronized (localAveragesBuffer) {
            return new ArrayList<>(localAveragesBuffer);
        }
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
