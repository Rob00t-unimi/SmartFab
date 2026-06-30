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
    public void testP2PPresentationIntegration() throws Exception {
        String serverUrl = "http://localhost:" + port;

        // 1. Start and register Node 1
        ProductionLine self1 = new ProductionLine(1, "127.0.0.1", 5001);
        ProductionLineNode node1 = createAndStartNode(self1, serverUrl);
        node1.registerWithAdminServer();
        node1.presentSelfToPeers();

        assertEquals(0, node1.getPeerCount()); // No other peers yet

        // 2. Start and register Node 2
        ProductionLine self2 = new ProductionLine(2, "127.0.0.1", 5002);
        ProductionLineNode node2 = createAndStartNode(self2, serverUrl);
        node2.registerWithAdminServer(); // Node 2 receives Node 1 from registry
        node2.presentSelfToPeers();      // Node 2 presents itself to Node 1 via gRPC

        // Give a brief moment for async executor thread to complete P2P presentation call
        Thread.sleep(200);

        // Verification:
        // Node 2 knows Node 1 (loaded from REST registry)
        assertEquals(1, node2.getPeerCount());
        assertTrue(node2.getPeers().stream().anyMatch(p -> p.id() == 1));

        // Node 1 knows Node 2 (received via gRPC present call)
        assertEquals(1, node1.getPeerCount());
        assertTrue(node1.getPeers().stream().anyMatch(p -> p.id() == 2));

        // 3. Start and register Node 3
        ProductionLine self3 = new ProductionLine(3, "127.0.0.1", 5003);
        ProductionLineNode node3 = createAndStartNode(self3, serverUrl);
        node3.registerWithAdminServer(); // Node 3 receives Node 1 and 2 from registry
        node3.presentSelfToPeers();      // Node 3 presents to Node 1 and 2 via gRPC

        // Give a brief moment for async executor threads to complete P2P presentation calls
        Thread.sleep(200);

        // Final verification:
        // Node 1 should know 2 and 3
        assertEquals(2, node1.getPeerCount());
        assertTrue(node1.getPeers().stream().anyMatch(p -> p.id() == 2));
        assertTrue(node1.getPeers().stream().anyMatch(p -> p.id() == 3));

        // Node 2 should know 1 and 3
        assertEquals(2, node2.getPeerCount());
        assertTrue(node2.getPeers().stream().anyMatch(p -> p.id() == 1));
        assertTrue(node2.getPeers().stream().anyMatch(p -> p.id() == 3));

        // Node 3 should know 1 and 2
        assertEquals(2, node3.getPeerCount());
        assertTrue(node3.getPeers().stream().anyMatch(p -> p.id() == 1));
        assertTrue(node3.getPeers().stream().anyMatch(p -> p.id() == 2));
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
