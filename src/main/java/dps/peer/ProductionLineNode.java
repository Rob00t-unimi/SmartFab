package dps.peer;

import dps.common.model.OperationalState;
import dps.common.model.ProductionLine;
import dps.peer.config.NodeConfig;
import dps.peer.net.NetworkManager;
import dps.peer.mqtt.MqttManager;
import dps.peer.coordinator.RicartAgrawalaCoordinator;
import dps.peer.sensor.SensorManager;
import org.w3c.dom.Node;

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

    public ProductionLineNode(NodeConfig config) {
        this(config.self(), config.serverUrl(), config.mqttBrokerUrl());
    }

    public ProductionLineNode(ProductionLine self, String serverUrl) {
        this(self, serverUrl, "tcp://localhost:1883");
    }

    public ProductionLineNode(ProductionLine self, String serverUrl, String mqttBrokerUrl) {
        this.self = self;
        this.serverUrl = serverUrl;
        this.mqttBrokerUrl = mqttBrokerUrl;

        this.networkManager = new NetworkManager(this, self, serverUrl);
        this.mqttManager = new MqttManager(this, self, mqttBrokerUrl);
        this.coordinator = new RicartAgrawalaCoordinator(this, self);
        this.sensorManager = new SensorManager(this, self, vibrationThreshold);
    }

    /** GETTERS **/
    public RicartAgrawalaCoordinator getCoordinator() { return coordinator; }
    public MqttManager getMqttManager() { return mqttManager; }
    public NetworkManager getNetworkManager() { return networkManager; }
    public SensorManager getSensorManager() { return sensorManager; }
    public ProductionLine getSelf() { return self; }
    public String getServerUrl() { return serverUrl; }
    public double getVibrationThreshold() { return vibrationThreshold; }

    /**
     * Checks if the physical sensor monitoring thread loop is currently running.
     */
    public boolean isRunning() { return sensorManager.isRunning(); }

    /** =================== STATE MANAGEMENT  ======================================================================================================================================================== **/

    public synchronized OperationalState getState() {
        return state;
    }
    public synchronized void setState(OperationalState state) {
        OperationalState oldState = this.state;
        this.state = state;
        if (oldState != state) {
            mqttManager.publishStateUpdate(state);  // immediatly notify server via MQTT
        }
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

    /** ============================================================================================================================================================================================= **/

    /**
     * Registers a JVM shutdown hook thread to clean up node resources gracefully.
     * Intercepts termination signals (like Ctrl+C or kill commands) to stop the local
     * gRPC server, release the TCP port, and halt the physical sensor monitoring loop.
     */
    public void startShutDownHookThread(ProductionLineNode node){
        // Create a final reference copy for the lambda environment capture
        final ProductionLineNode finalNode = node;
        // Register a shutdown hook thread with the JVM to intercept termination signals (Ctrl+C, kill)
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[PEER " + finalNode.getSelf().id() + "] SHUTDOWN HOOK triggered. Stopping gRPC server and monitoring...");
            // Stop the local gRPC server Netty instance to free the TCP socket port
            finalNode.getNetworkManager().stopGrpcServer();
            // Stop the background consumer monitoring thread
            finalNode.getSensorManager().stopMonitoring();
            // Stop the MQTT client connection
            finalNode.getMqttManager().stop();
            // Stop the coordinator's thread pool
            finalNode.getCoordinator().stop();
        }, "Shutdown-Hook-Node-" + node.getSelf().id()));
    }

    public static void main(String[] args) {
        ProductionLineNode node = null;
        try {
            NodeConfig config = NodeConfig.parseArgs(args);
            // Redirect console output to LogUtils for clean colorized and shared logs
            dps.common.util.LogUtils.redirectSystemOutAndErr("PEER-" + config.self().id(), dps.common.util.LogUtils.getPeerColor(String.valueOf(config.self().id())));
            
            node = new ProductionLineNode(config);

            System.out.println("[PEER " + node.getSelf().id() + "] STARTING NODE on " + node.getSelf().ip() + ":" + node.getSelf().port());
            System.out.println("[PEER " + node.getSelf().id() + "] Admin Server URL: " + node.getServerUrl());

           // Start a thread to monitoring and executing a clean shutdown
            node.startShutDownHookThread(node);

            // Start local gRPC server
            node.getNetworkManager().startGrpcServer();

            // Contact Administration Server to register and obtain current peer list
            node.getNetworkManager().registerWithAdminServer();

            // Introduce self via gRPC to each active peer
            node.getNetworkManager().presentSelfToPeers();

            // Connect to MQTT Broker and publish initial ONLINE state
            node.getMqttManager().connectMqtt();

            // Start physical sensor simulation loop and consumer thread
            node.getSensorManager().startMonitoring();

            System.out.println("[PEER " + node.getSelf().id() + "] Node is running. Press Ctrl+C to exit.");

            // Block and suspend the main thread to prevent the JVM from exiting prematurely.
            // This keeps the process running while background gRPC and MQTT threads handle events,
            // and will unblock only when the gRPC server is shut down (e.g., via the JVM shutdown hook).
            node.getNetworkManager().blockUntilGRPCShutdown();

        } catch (IllegalArgumentException e) {
            System.err.println("ERROR: " + e.getMessage());
            System.err.println("USAGE: java dps.peer.ProductionLineNode <id> <ip> <port> [serverUrl] [mqttBrokerUrl]");
            System.exit(1);
        } catch (IllegalStateException e) {
            System.err.println("STARTUP FAILED: " + e.getMessage());
            if (node != null) {
                node.getNetworkManager().stopGrpcServer();
                node.getSensorManager().stopMonitoring();
                node.getMqttManager().stop();
                node.getCoordinator().stop();
            }
            System.exit(1);
        } catch (Exception e) {
            System.err.println("UNEXPECTED ERROR: " + e.getMessage());
            if (node != null) {
                node.getNetworkManager().stopGrpcServer();
                node.getSensorManager().stopMonitoring();
                node.getMqttManager().stop();
                node.getCoordinator().stop();
            }
            System.exit(1);
        }
    }
}
