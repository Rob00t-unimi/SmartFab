package dps.adminServer;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

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

    private final String clientId = "smartfab-admin-server";

    public MqttSubscriberService(ProductionLineRegistry registry) {
        this.registry = registry;
    }

    @PostConstruct
    public void start() {
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
        // Will implement JSON parsing and registry updates in Commit 2
        System.out.println("[ADMIN_SERVER] Received MQTT message on topic: " + topic);
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // Used only for publishing, ignored on subscriber
    }
}
