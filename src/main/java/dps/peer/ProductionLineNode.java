package dps.peer;

import dps.common.model.OperationalState;
import dps.common.model.ProductionLine;
import dps.peer.config.NodeConfig;
import dps.peer.net.NetworkManager;
import dps.peer.mqtt.MqttManager;
import dps.peer.coordinator.RicartAgrawalaCoordinator;
import dps.peer.sensor.SensorManager;

/**
 * Main orchestrator for a Production Line node in the SmartFab network.
 * Coordinates network, MQTT telemetry, and Ricart-Agrawala mutual exclusion modules.
 */
public class ProductionLineNode {

    private final ProductionLine self;
    private final String serverUrl;
    private final String mqttBrokerUrl;
    
    private final NetworkManager networkManager;
    private final MqttManager mqttManager;
    private final RicartAgrawalaCoordinator coordinator;
    private final SensorManager sensorManager;

    private OperationalState state = OperationalState.FULLY_OPERATIONAL;
    private final double vibrationThreshold = 80.0;

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
        this.sensorManager = new SensorManager(this, self, vibrationThreshold);
    }

    public RicartAgrawalaCoordinator getCoordinator() {
        return coordinator;
    }

    public MqttManager getMqttManager() {
        return mqttManager;
    }

    public NetworkManager getNetworkManager() {
        return networkManager;
    }

    public SensorManager getSensorManager() {
        return sensorManager;
    }

    public ProductionLine getSelf() {
        return self;
    }

    public double getVibrationThreshold() {
        return vibrationThreshold;
    }

    public boolean isRunning() {
        return sensorManager.isRunning();
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

    public double calcCriticality() {
        return (coordinator.getLastCalculatedAverage() - vibrationThreshold) / vibrationThreshold;
    }

    public String getServerUrl() {
        return serverUrl;
    }

    /**
     * Starts the physical sensor simulator and the background thread that consumes
     * measurements from the sliding window buffer. Starts mqtt publishing.
     */
    public synchronized void startMonitoring() {
        sensorManager.startMonitoring();
    }

    /**
     * Stops the sensor simulator and shuts down the background monitoring thread and mqtt publisher.
     */
    public synchronized void stopMonitoring() {
        sensorManager.stopMonitoring();
        mqttManager.stop();
    }

    /**
     * Local state transition to WAITING_FOR_CALIBRATION.
     * Pauses the physical sensor simulator and clears the window buffer.
     */
    public synchronized void transitionToWaitingForCalibration(double average) {
        setState(OperationalState.WAITING_FOR_CALIBRATION);

        System.out.println("[PEER " + self.id() + "] [WAITING_FOR_CALIBRATION] ⚠️ Average vibration " 
                + String.format("%.2f", average) + " exceeded threshold " + vibrationThreshold + "! Pausing sensor, clearing buffer, and requesting calibration...");

        sensorManager.pauseMeasuring();
        sensorManager.clearBuffer();

        // Asynchronously start the calibration sequence in a separate thread
        new Thread(() -> {
            coordinator.enterCalibrationAndWait();
            coordinator.releaseCalibration();
        }, "Calibration-Coordinator-Thread-Node-" + self.id()).start();
    }

    public static void main(String[] args) {
        ProductionLineNode node = null;
        try {
            NodeConfig config = NodeConfig.parseArgs(args);
            node = new ProductionLineNode(config.self(), config.serverUrl());

            // Add JVM shutdown hook to clean up resources gracefully
            final ProductionLineNode finalNode = node;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("[PEER " + finalNode.getSelf().id() + "] Shutdown hook triggered. Stopping gRPC server and monitoring...");
                finalNode.getNetworkManager().stopGrpcServer();
                finalNode.stopMonitoring();
            }, "Shutdown-Hook-Node-" + config.self().id()));

            // Start local gRPC server
            node.getNetworkManager().startGrpcServer();

            // Contact Administration Server to register and obtain current peer list
            node.getNetworkManager().registerWithAdminServer();

            // Introduce self via gRPC to each active peer
            node.getNetworkManager().presentSelfToPeers();

            // Connect to MQTT Broker and publish initial ONLINE state
            node.getMqttManager().connectMqtt();

            // Start physical sensor simulation loop and consumer thread
            node.startMonitoring();

            // Keep the main thread alive waiting for server shutdown
            node.getNetworkManager().blockUntilGRPCShutdown();

        } catch (IllegalArgumentException e) {
            System.err.println("Configuration Error: " + e.getMessage());
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
}
