package dps.peer;

import dps.common.model.ProductionLine;
import dps.adminServer.ProductionLineRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.annotation.DirtiesContext;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = dps.adminServer.AdminServerApp.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class ProductionLineNodeIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private ProductionLineRegistry registry;

    private final List<ProductionLineNode> startedNodes = new ArrayList<>();

    @AfterEach
    public void tearDown() {
        for (ProductionLineNode node : startedNodes) {
            try {
                node.stopGrpcServer();
            } catch (Exception e) {
                // Ignore failure during shutdown of single test server
            }
        }
        startedNodes.clear();
    }

    private ProductionLineNode createAndStartNode(ProductionLine self, String serverUrl) throws Exception {
        ProductionLineNode node = new ProductionLineNode(self, serverUrl);
        node.startGrpcServer();
        startedNodes.add(node);
        return node;
    }

    @Test
    public void testEndToEndRegistration() throws Exception {
        String serverUrl = "http://localhost:" + port;

        // Register Node 1
        ProductionLine self1 = new ProductionLine(1, "127.0.0.1", 5001);
        ProductionLineNode node1 = createAndStartNode(self1, serverUrl);

        assertEquals(0, node1.getPeerCount());
        node1.registerWithAdminServer();

        // Node 1 is registered, list of returned peers should be empty
        assertEquals(0, node1.getPeerCount());

        // Register Node 2
        ProductionLine self2 = new ProductionLine(2, "127.0.0.1", 5002);
        ProductionLineNode node2 = createAndStartNode(self2, serverUrl);

        assertEquals(0, node2.getPeerCount());
        node2.registerWithAdminServer();

        // Node 2 is registered, should have loaded Node 1 as a peer
        assertEquals(1, node2.getPeerCount());
        assertEquals(1, node2.getPeers().get(0).id());

        // Attempting to register Node 1 again should fail with Conflict
        ProductionLine self1Duplicate = new ProductionLine(1, "127.0.0.1", 9999);
        ProductionLineNode node1Duplicate = new ProductionLineNode(self1Duplicate, serverUrl);

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            node1Duplicate.registerWithAdminServer();
        });
        assertTrue(exception.getMessage().contains("Registration conflict"));
    }

    @Test
    public void testServerOffline() {
        // Use an unreachable port
        String serverUrl = "http://localhost:59999";
        ProductionLine self = new ProductionLine(10, "127.0.0.1", 5010);
        ProductionLineNode node = new ProductionLineNode(self, serverUrl);

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            node.registerWithAdminServer();
        });
        assertTrue(exception.getMessage().contains("Connection failed"));
    }
}
