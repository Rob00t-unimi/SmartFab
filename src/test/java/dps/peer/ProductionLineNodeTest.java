package dps.peer;

import dps.common.model.OperationalState;
import dps.common.model.ProductionLine;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

public class ProductionLineNodeTest {

    @Test
    public void testValidArgumentsWithDefaultServerUrl() {
        String[] args = {"1", "127.0.0.1", "5001"};
        ProductionLineNode.NodeConfig config = ProductionLineNode.parseArgs(args);

        assertNotNull(config);
        assertEquals(1, config.self().id());
        assertEquals("127.0.0.1", config.self().ip());
        assertEquals(5001, config.self().port());
        assertEquals("http://localhost:8080", config.serverUrl());
    }

    @Test
    public void testValidArgumentsWithCustomServerUrl() {
        String[] args = {"2", "localhost", "5002", "http://192.168.1.100:9000"};
        ProductionLineNode.NodeConfig config = ProductionLineNode.parseArgs(args);

        assertNotNull(config);
        assertEquals(2, config.self().id());
        assertEquals("localhost", config.self().ip());
        assertEquals(5002, config.self().port());
        assertEquals("http://192.168.1.100:9000", config.serverUrl());
    }

    @Test
    public void testInsufficientArguments() {
        String[] args = {"1", "127.0.0.1"};
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            ProductionLineNode.parseArgs(args);
        });
        assertTrue(exception.getMessage().contains("Insufficient arguments"));
    }

    @Test
    public void testInvalidIdFormat() {
        String[] args = {"abc", "127.0.0.1", "5001"};
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            ProductionLineNode.parseArgs(args);
        });
        assertTrue(exception.getMessage().contains("ID must be an integer"));
    }

    @Test
    public void testInvalidPortFormat() {
        String[] args = {"1", "127.0.0.1", "xyz"};
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            ProductionLineNode.parseArgs(args);
        });
        assertTrue(exception.getMessage().contains("Port must be an integer"));
    }

    @Test
    public void testNegativeId() {
        String[] args = {"-1", "127.0.0.1", "5001"};
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            ProductionLineNode.parseArgs(args);
        });
        assertTrue(exception.getMessage().contains("ID must be non-negative"));
    }

    @Test
    public void testInvalidIpAddress() {
        String[] args = {"1", "999.999.999.999", "5001"};
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            ProductionLineNode.parseArgs(args);
        });
        assertTrue(exception.getMessage().contains("Invalid IP address format"));
    }

    @Test
    public void testInvalidPortRange() {
        String[] args = {"1", "127.0.0.1", "80"};
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            ProductionLineNode.parseArgs(args);
        });
        assertTrue(exception.getMessage().contains("Port must be between 1024 and 65535"));
    }

    @Test
    public void testPeerManagementBasic() {
        ProductionLine self = new ProductionLine(1, "127.0.0.1", 5001);
        ProductionLineNode node = new ProductionLineNode(self, "http://localhost:8080");

        assertEquals(0, node.getPeerCount());
        assertTrue(node.getPeers().isEmpty());

        ProductionLine peer1 = new ProductionLine(2, "127.0.0.1", 5002);
        ProductionLine peer2 = new ProductionLine(3, "127.0.0.1", 5003);

        node.addPeer(peer1);
        node.addPeer(peer2);

        assertEquals(2, node.getPeerCount());
        List<ProductionLine> activePeers = node.getPeers();
        assertEquals(2, activePeers.size());
        assertTrue(activePeers.contains(peer1));
        assertTrue(activePeers.contains(peer2));

        node.removePeer(2);
        assertEquals(1, node.getPeerCount());
        assertFalse(node.getPeers().contains(peer1));
        assertTrue(node.getPeers().contains(peer2));
    }

    @Test
    public void testPeerManagementConcurrent() throws InterruptedException {
        ProductionLine self = new ProductionLine(1, "127.0.0.1", 5001);
        ProductionLineNode node = new ProductionLineNode(self, "http://localhost:8080");

        int threadCount = 100;
        Thread[] threads = new Thread[threadCount];
        AtomicBoolean failed = new AtomicBoolean(false);

        for (int i = 0; i < threadCount; i++) {
            final int id = i + 10;
            threads[i] = new Thread(() -> {
                try {
                    ProductionLine p = new ProductionLine(id, "127.0.0.1", 5000 + id);
                    node.addPeer(p);
                    // Perform concurrent reads
                    node.getPeers();
                    node.getPeerCount();
                    // Intermittently remove some peers to test concurrent removals
                    if (id % 2 == 0) {
                        node.removePeer(id);
                    }
                } catch (Exception e) {
                    failed.set(true);
                }
            });
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) t.join();

        assertFalse(failed.get(), "Concurrent peer modifications caused exceptions or data corruption");
        // Count should be exactly half of the threads that didn't get removed (id % 2 != 0)
        // Which is threadCount / 2 = 50 peers remaining.
        assertEquals(50, node.getPeerCount());
    }

    @Test
    public void testInitialState() {
        ProductionLine self = new ProductionLine(1, "127.0.0.1", 5001);
        ProductionLineNode node = new ProductionLineNode(self, "http://localhost:8080");

        assertNotNull(node.getBuffer());
        assertNotNull(node.getSensor());
        assertEquals(OperationalState.FULLY_OPERATIONAL, node.getState());
    }

    @Test
    public void testMonitoringLoopCalculatesAverageWithoutTransition() throws InterruptedException {
        ProductionLine self = new ProductionLine(1, "127.0.0.1", 5001);
        ProductionLineNode node = new ProductionLineNode(self, "http://localhost:8080");

        assertEquals(OperationalState.FULLY_OPERATIONAL, node.getState());

        // Start monitoring loop
        node.startMonitoring();
        node.getSensor().pauseMeasuring();
        node.getBuffer().clear();

        // Feed 8 measurements with values > 80.0
        long now = System.currentTimeMillis();
        for (int i = 0; i < 8; i++) {
            node.getBuffer().addMeasurement(new sensor.Measurement("Vibration-1", "Vibration", 90.0, now + i));
        }

        // Wait for consumer thread to consume and calculate
        Thread.sleep(300);

        // Verify that the state remains FULLY_OPERATIONAL since threshold logic is not active in Commit 2
        assertEquals(OperationalState.FULLY_OPERATIONAL, node.getState());

        node.stopMonitoring();
    }
}
