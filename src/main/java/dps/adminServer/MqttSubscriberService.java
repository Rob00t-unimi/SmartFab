package dps.adminServer;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dps.common.model.OperationalState;

/**
 * Background service that subscribes to peer MQTT telemetry and status updates.
 * Automatically started by AdminServerApp (spring)
 */
@Service
public class MqttSubscriberService implements MqttCallback {

    private final ProductionLineRegistry registry;
    private MqttClient mqttClient;

    @Value("${mqtt.broker.url:tcp://localhost:1883}")
    private String brokerUrl;

    private final String clientId = "smartfab-admin-server-" + java.util.UUID.randomUUID().toString().substring(0, 8);

    public MqttSubscriberService(ProductionLineRegistry registry) {
        this.registry = registry;
    }

    @PostConstruct
    public void start() {
        // Redirect server-side standard console output to LogUtils
        dps.common.util.LogUtils.redirectSystemOutAndErr("SERVER", dps.common.util.LogUtils.ANSI_CYAN);
        try {
            mqttClient = new MqttClient(brokerUrl, clientId, new MemoryPersistence());
            mqttClient.setCallback(this);

            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            connOpts.setAutomaticReconnect(true); // Automatically reconnects if broker drops

            System.out.println("[ADMIN_SERVER] Connecting to MQTT Broker: " + brokerUrl + "...");
            mqttClient.connect(connOpts);
            System.out.println("[ADMIN_SERVER] Connected to MQTT Broker successfully.");

            // Subscribe to wildcards
            String telemetryTopic = "smartfab/production-line/+/telemetry";
            String statusTopic = "smartfab/production-line/+/status";

            mqttClient.subscribe(telemetryTopic, 1);
            mqttClient.subscribe(statusTopic, 1);
            System.out.println("[ADMIN_SERVER] Subscribed to MQTT topics: " + telemetryTopic + " and " + statusTopic);

        } catch (MqttException e) {
            System.err.println("[ADMIN_SERVER] ❌ Failed to start MQTT Subscriber: " + e.getMessage());
        }
    }

    @PreDestroy
    public void stop() {
        if (mqttClient != null && mqttClient.isConnected()) {
            try {
                System.out.println("[ADMIN_SERVER] Disconnecting from MQTT Broker...");
                mqttClient.disconnect();
                mqttClient.close();
                System.out.println("[ADMIN_SERVER] Disconnected from MQTT Broker successfully.");
            } catch (MqttException e) {
                System.err.println("[ADMIN_SERVER] ❌ Error disconnecting MQTT Subscriber: " + e.getMessage());
            }
        }
    }

    // MqttCallback implementation methods
    @Override
    public void connectionLost(Throwable cause) {
        System.err.println("[ADMIN_SERVER] ⚠️ MQTT Connection lost: " + (cause != null ? cause.getMessage() : "unknown"));
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        String payload = new String(message.getPayload());
        System.out.println("[ADMIN_SERVER] Received MQTT message on topic: " + topic);

        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(payload);
            int id = root.get("id").asInt();

            // state update
            if (topic.endsWith("/status")) {
                String stateStr = root.get("state").asText();
                OperationalState state = OperationalState.valueOf(stateStr);
                registry.updateState(id, state);    // update on registry
                System.out.println("[ADMIN_SERVER] [MQTT-STATUS] Updated state of node " + id + " to " + state);

            // Update registry with all telemetries
            } else if (topic.endsWith("/telemetry")) {
                long timestamp = root.get("timestamp").asLong();
                JsonNode averagesNode = root.get("averages");
                if (averagesNode != null && averagesNode.isArray()) {
                    int count = 0;
                    for (JsonNode avgNode : averagesNode) {
                        double avg = avgNode.asDouble();
                        registry.addTelemetry(id, avg, timestamp);
                        count++;
                    }
                    System.out.println("[ADMIN_SERVER] [MQTT-TELEMETRY] Added " + count + " telemetry average(s) for node " + id);
                }
            }
        } catch (Exception e) {
            System.err.println("[ADMIN_SERVER] ❌ Error parsing MQTT payload on topic " + topic + ": " + e.getMessage());
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // Used only for publishing, ignored on subscriber
    }
}
