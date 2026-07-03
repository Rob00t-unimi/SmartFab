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
     * Connects to the MQTT Broker
     */
    public synchronized void connectMqtt() {
        if (mqttClient != null && mqttClient.isConnected()) {
            return;
        }

        try {
            String clientId = "PEER-" + self.id();
            // instantiate client mqtt with eclipse paho.
            mqttClient = new MqttClient(mqttBrokerUrl, clientId, new MemoryPersistence());  // memory persistence keeps temporary messages not yet sent in RAM instead of in the FS
            MqttConnectOptions connOpts = new MqttConnectOptions();  // config connection parameters
            connOpts.setCleanSession(true);

            System.out.println("[PEER " + self.id() + "] [" + node.getState() + "] Connecting to MQTT Broker: " + mqttBrokerUrl + "...");
            mqttClient.connect(connOpts);   // open connection (wait broker response)
            System.out.println("[PEER " + self.id() + "] [" + node.getState() + "] Connected to MQTT Broker successfully.");
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
                System.out.println("[PEER " + self.id() + "] [" + node.getState() + "] Disconnecting from MQTT Broker...");
                mqttClient.disconnect();
                mqttClient.close();
                System.out.println("[PEER " + self.id() + "] [" + node.getState() + "] Disconnected from MQTT Broker successfully.");
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
    public void publishStateUpdate(OperationalState newState) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode rootNode = mapper.createObjectNode();
            rootNode.put("id", self.id());
            rootNode.put("state", newState.name());
            rootNode.put("timestamp", System.currentTimeMillis());
            
            if (newState == OperationalState.WAITING_FOR_CALIBRATION || newState == OperationalState.UNDER_CALIBRATION) {
                rootNode.put("criticality", node.calcCriticality());
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
                    rootNode.put("state", node.getState().name());
                    
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
     * Buffers a computed average for periodic telemetry.
     */
    public void addAverage(double average) {
        synchronized (localAveragesBuffer) {
            localAveragesBuffer.add(average);
        }
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
