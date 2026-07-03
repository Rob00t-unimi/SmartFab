package dps.adminClient;

import dps.common.model.OperationalState;
import dps.common.model.ProductionLine;
import dps.common.model.ProductionLineStatus;
import dps.adminServer.service.ProductionLineRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.annotation.DirtiesContext;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the AdminClientApp REST client.
 */
@SpringBootTest(classes = dps.adminServer.AdminServerApp.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class AdminClientAppTest {

    @LocalServerPort
    private int port;

    @Autowired
    private ProductionLineRegistry registry;

    private AdminClientApp client;

    @BeforeEach
    public void setUp() {
        String serverUrl = "http://localhost:" + port;
        client = new AdminClientApp(serverUrl);
    }

    @Test
    public void testGetLinesStatusIntegration() {
        ProductionLine line1 = new ProductionLine(1, "127.0.0.1", 5001);
        ProductionLine line2 = new ProductionLine(2, "127.0.0.1", 5002);
        
        registry.register(line1);
        registry.register(line2);
        registry.updateState(2, OperationalState.WAITING_FOR_CALIBRATION);

        // Fetch status using the Admin Client
        List<ProductionLineStatus> statuses = client.getLinesStatus();

        assertEquals(2, statuses.size());
        
        // Assert line 1 status
        ProductionLineStatus status1 = statuses.stream()
                .filter(s -> s.line().id() == 1)
                .findFirst()
                .orElseThrow();
        assertEquals(OperationalState.FULLY_OPERATIONAL, status1.state());

        // Assert line 2 status
        ProductionLineStatus status2 = statuses.stream()
                .filter(s -> s.line().id() == 2)
                .findFirst()
                .orElseThrow();
        assertEquals(OperationalState.WAITING_FOR_CALIBRATION, status2.state());
    }

    @Test
    public void testGetAverageVibrationIntegration() {
        int id = 10;
        ProductionLine line = new ProductionLine(id, "127.0.0.1", 5010);
        registry.register(line);

        long now = System.currentTimeMillis();
        registry.addTelemetry(id, 80.0, now - 5000);
        registry.addTelemetry(id, 120.0, now - 2000);
        registry.addTelemetry(id, 200.0, now + 5000); // Out of range

        // Query average using the Admin Client
        double avg = client.getAverageVibration(id, now - 6000, now);

        assertEquals(100.0, avg, 0.001); // (80 + 120) / 2 = 100
    }

    @Test
    public void testGetAverageVibrationNotFoundIntegration() {
        // Query average for a non-existing node
        double avg = client.getAverageVibration(999, 0L, 1000L);
        assertEquals(0.0, avg, 0.001);
    }
}
