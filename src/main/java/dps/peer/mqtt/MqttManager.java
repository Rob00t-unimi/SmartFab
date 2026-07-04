package dps.peer.mqtt;

import dps.common.model.OperationalState;
import dps.common.model.ProductionLine;
import dps.peer.ProductionLineNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.util.ArrayList;
import java.util.List;

/**
 * Manages the MQTT connection, telemetry loop (publishing averages every 10 seconds),
 * and immediate status change notifications.
 */
public class MqttManager {

    private final ProductionLineNode node;
    private final ProductionLine self;
    private final String mqttBrokerUrl;

    // Single Jackson ObjectMapper instance reused for performance and thread-safety
    private final ObjectMapper mapper = new ObjectMapper();

    // MQTT client configuration
    private MqttClient mqttClient;

    // MQTT buffer and publisher thread
    private final List<Double> localAveragesBuffer = new ArrayList<>();     // local averages from sensor to send via mqtt
    private Thread mqttPublisherThread; // 10 seconds cycle mqtt thread

    public MqttManager(ProductionLineNode node, ProductionLine self, String mqttBrokerUrl) {
        if (node == null) {
            throw new IllegalArgumentException("ProductionLineNode reference cannot be null.");
        }
        this.node = node;
        this.self = self;
        this.mqttBrokerUrl = mqttBrokerUrl;
    }

    /**
     * Builds the common JSON fields present in all SmartFab MQTT payloads.
     */
    private ObjectNode createBasePayload(OperationalState state) {
        ObjectNode rootNode = mapper.createObjectNode();
        rootNode.put("id", self.id());
        rootNode.put("state", state.name());
        rootNode.put("timestamp", System.currentTimeMillis());
        return rootNode;
    }

    /**
     * Helper to construct telemetry JSON string.
     */
    private String buildTelemetryPayload(List<Double> averages) throws Exception {
        ObjectNode rootNode = createBasePayload(node.getState());
        ArrayNode averagesArray = mapper.createArrayNode();
        for (double avg : averages) {
            averagesArray.add(avg);
        }
        rootNode.set("averages", averagesArray);
        return mapper.writeValueAsString(rootNode);
    }

    /**
     * Buffers a computed average for periodic telemetry.
     */
    public void addAverage(double average) {
        synchronized (localAveragesBuffer) {
            localAveragesBuffer.add(average);
        }
    }

    /**
     * Publishes telemetry payload to the MQTT Broker.
     */
    public void publishTelemetry(String payload) {
        publishToTopic("telemetry", payload);
    }

    /**
     * Publishes status payload to the MQTT Broker.
     */
    public void publishStatus(String payload) {
        publishToTopic("status", payload);
    }

    /**
     * Connects to the MQTT Broker
     */
    public void connectMqtt() {
        synchronized (this) {
            if (mqttClient != null && mqttClient.isConnected()) {
                return;
            }
        }
        try {
            // MemoryPersistence indicates that the client stores messages in volatile memory rather than on disk
            MemoryPersistence persistence = new MemoryPersistence();
            String clientId = "SmartFab-Node-" + self.id();
            MqttClient client = new MqttClient(mqttBrokerUrl, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true); // forget past subscription history
            connOpts.setAutomaticReconnect(true); // auto reconnect if network drops

            System.out.println("[PEER " + self.id() + "] Connecting to MQTT Broker at " + mqttBrokerUrl + "...");
            client.connect(connOpts);
            System.out.println("[PEER " + self.id() + "] Connected to MQTT Broker successfully.");

            synchronized (this) {
                this.mqttClient = client;
            }

            // Publish initial state update to register online
            publishStateUpdate(node.getState());

        } catch (Exception e) {
            System.err.println("[PEER " + self.id() + "] ❌ Failed to connect to MQTT Broker: " + e.getMessage());
        }
    }

    /**
     * Disconnects from the MQTT Broker
     */
    public void disconnectMqtt() {
        MqttClient client;
        synchronized (this) {
            client = this.mqttClient;
            this.mqttClient = null;
        }
        if (client != null && client.isConnected()) {
            try {
                System.out.println("[PEER " + self.id() + "] Disconnecting from MQTT Broker...");
                client.disconnect();
                client.close();
                System.out.println("[PEER " + self.id() + "] Disconnected from MQTT Broker successfully.");
            } catch (Exception e) {
                System.err.println("[PEER " + self.id() + "] Error while disconnecting MQTT: " + e.getMessage());
            }
        }
    }

    /**
     * Generic private publishing method.
     */
    private void publishToTopic(String subTopic, String payload) {
        synchronized (this) {
            if (mqttClient == null || !mqttClient.isConnected()) {
                System.err.println("[PEER " + self.id() + "] Cannot publish " + subTopic + ": MQTT client not connected.");
                return;
            }
        }
        try {
            String topic = "smartfab/production-line/" + self.id() + "/" + subTopic;
            MqttMessage message = new MqttMessage(payload.getBytes());
            message.setQos(1);  // QoS 1 guarantees delivery at least once
            mqttClient.publish(topic, message);
        } catch (Exception e) {
            System.err.println("[PEER " + self.id() + "] ❌ Error publishing MQTT " + subTopic + ": " + e.getMessage());
        }
    }

    /**
     * Builds and asynchronously publishes state change update.
     */
    public void publishStateUpdate(OperationalState newState) {
        try {
            ObjectNode rootNode = createBasePayload(newState);
            
            if (newState == OperationalState.WAITING_FOR_CALIBRATION || newState == OperationalState.UNDER_CALIBRATION) {
                rootNode.put("criticality", node.getCoordinator().calcCriticality());
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

    /**
     * Starts the periodic thread that publishes collected averages every 10 seconds.
     */
    public void startMqttTelemetryPublishing() {
        mqttPublisherThread = new Thread(() -> {
            while (node.isRunning()) {
                try {
                    Thread.sleep(10000); // Wait 10 seconds
                    
                    // Extract and clear the local averages buffer thread-safely
                    List<Double> averagesToPublish;
                    synchronized (localAveragesBuffer) {
                        averagesToPublish = new ArrayList<>(localAveragesBuffer);
                        localAveragesBuffer.clear();
                    }
                    
                    String payload = buildTelemetryPayload(averagesToPublish);
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
     * Stops the publisher thread and disconnects.
     */
    public void stop() {
        // The MQTT publishing thread is safely stopped via interruption
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
    }

    /**
     * Helper to retrieve buffered averages for testing/verification.
     */
    public List<Double> getLocalAveragesBufferSnapshot() {
        synchronized (localAveragesBuffer) {
            return new ArrayList<>(localAveragesBuffer);
        }
    }
}
