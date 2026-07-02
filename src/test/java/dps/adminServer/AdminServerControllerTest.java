package dps.adminServer;

import com.fasterxml.jackson.databind.ObjectMapper;
import dps.common.model.OperationalState;
import dps.common.model.ProductionLine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.annotation.DirtiesContext;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.hamcrest.Matchers.*;

@SpringBootTest
@AutoConfigureMockMvc
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class AdminServerControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ProductionLineRegistry registry;

    @Test
    public void testRegisterAndGetLinesStatus() throws Exception {
        ProductionLine line1 = new ProductionLine(1, "127.0.0.1", 5001);
        ProductionLine line2 = new ProductionLine(2, "127.0.0.1", 5002);

        // Register first line
        mockMvc.perform(post("/production-lines")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(line1)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(0)));

        // Register second line, should return first line as peer
        mockMvc.perform(post("/production-lines")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(line2)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(1)))
                .andExpect(jsonPath("$[0].id", is(1)));

        // Get all lines status (using the new DTO structure)
        mockMvc.perform(get("/production-lines"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(2)))
                .andExpect(jsonPath("$[0].line.id", is(1)))
                .andExpect(jsonPath("$[0].state", is("FULLY_OPERATIONAL")))
                .andExpect(jsonPath("$[1].line.id", is(2)))
                .andExpect(jsonPath("$[1].state", is("FULLY_OPERATIONAL")));
    }

    @Test
    public void testDuplicateRegistration() throws Exception {
        ProductionLine line3 = new ProductionLine(3, "127.0.0.1", 5003);

        mockMvc.perform(post("/production-lines")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(line3)))
                .andExpect(status().isOk());

        mockMvc.perform(post("/production-lines")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(line3)))
                .andExpect(status().isConflict());
    }

    @Test
    public void testGetStatsNotFound() throws Exception {
        mockMvc.perform(get("/production-lines/999/stats")
                .param("t1", "0")
                .param("t2", "1000"))
                .andExpect(status().isNotFound())
                .andExpect(content().string(containsString("not found")));
    }

    @Test
    public void testGetAverageVibration() throws Exception {
        int id = 10;
        ProductionLine line = new ProductionLine(id, "127.0.0.1", 5010);
        
        // Manual registration through registry for testing
        registry.register(line);
        
        long now = System.currentTimeMillis();
        registry.addTelemetry(id, 50.0, now - 5000);
        registry.addTelemetry(id, 100.0, now - 2000);
        registry.addTelemetry(id, 200.0, now + 5000); // Outside range

        mockMvc.perform(get("/production-lines/" + id + "/stats")
                .param("t1", String.valueOf(now - 6000))
                .param("t2", String.valueOf(now)))
                .andExpect(status().isOk())
                .andExpect(content().string("75.0")); // (50 + 100) / 2
    }

    @Test
    public void testUpdateState() throws Exception {
        int id = 20;
        registry.register(new ProductionLine(id, "127.0.0.1", 5020));
        
        registry.updateState(id, OperationalState.WAITING_FOR_CALIBRATION);
        
        mockMvc.perform(get("/production-lines"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].line.id", is(id)))
                .andExpect(jsonPath("$[0].state", is("WAITING_FOR_CALIBRATION")));
    }

    @Test
    public void testMqttIntegrationWithRegistry() throws Exception {
        int id = 50;
        ProductionLine line = new ProductionLine(id, "127.0.0.1", 5050);
        registry.register(line);

        // Verify initial state is FULLY_OPERATIONAL
        mockMvc.perform(get("/production-lines"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].line.id", is(id)))
                .andExpect(jsonPath("$[0].state", is("FULLY_OPERATIONAL")));

        // Create temporary MQTT client to publish updates to localhost broker
        org.eclipse.paho.client.mqttv3.MqttClient tempMqttClient = 
                new org.eclipse.paho.client.mqttv3.MqttClient("tcp://localhost:1883", "smartfab-test-publisher", new org.eclipse.paho.client.mqttv3.persist.MemoryPersistence());
        tempMqttClient.connect();

        // 1. Publish status update
        String statusPayload = "{\"id\":" + id + ",\"state\":\"WAITING_FOR_CALIBRATION\",\"timestamp\":" + System.currentTimeMillis() + "}";
        org.eclipse.paho.client.mqttv3.MqttMessage statusMsg = new org.eclipse.paho.client.mqttv3.MqttMessage(statusPayload.getBytes());
        statusMsg.setQos(1);
        tempMqttClient.publish("smartfab/production-line/" + id + "/status", statusMsg);

        // 2. Publish telemetry averages list
        long now = System.currentTimeMillis();
        String telemetryPayload = "{\"id\":" + id + ",\"averages\":[65.5,75.5],\"timestamp\":" + now + "}";
        org.eclipse.paho.client.mqttv3.MqttMessage telemetryMsg = new org.eclipse.paho.client.mqttv3.MqttMessage(telemetryPayload.getBytes());
        telemetryMsg.setQos(1);
        tempMqttClient.publish("smartfab/production-line/" + id + "/telemetry", telemetryMsg);

        tempMqttClient.disconnect();
        tempMqttClient.close();

        // Wait a brief moment for the subscriber to receive and process messages
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < 3000) {
            // Verify that the state was updated to WAITING_FOR_CALIBRATION in the registry
            if (registry.getLinesStatus().get(0).state() == OperationalState.WAITING_FOR_CALIBRATION) {
                break;
            }
            Thread.sleep(100);
        }

        // Assert state update
        mockMvc.perform(get("/production-lines"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].line.id", is(id)))
                .andExpect(jsonPath("$[0].state", is("WAITING_FOR_CALIBRATION")));

        // Assert average vibration query REST endpoint (65.5 + 75.5) / 2 = 70.5
        mockMvc.perform(get("/production-lines/" + id + "/stats")
                .param("t1", String.valueOf(now - 10000))
                .param("t2", String.valueOf(now + 10000)))
                .andExpect(status().isOk())
                .andExpect(content().string("70.5"));
    }
}
