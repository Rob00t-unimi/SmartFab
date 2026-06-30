package dps.peer;

import dps.common.model.OperationalState;
import dps.common.model.ProductionLine;
import dps.peer.proto.CalibrationRequest;
import dps.peer.proto.CalibrationReply;
import io.grpc.stub.StreamObserver;
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
            node.getBuffer().addMeasurement(new sensor.Measurement("Vibration-1", "Vibration", 50.0, now + i));
        }

        // Wait for consumer thread to consume and calculate
        Thread.sleep(300);

        // Verify that the state remains FULLY_OPERATIONAL since threshold logic is not active in Commit 2
        assertEquals(OperationalState.FULLY_OPERATIONAL, node.getState());

        node.stopMonitoring();
    }

    @Test
    public void testMonitoringLoopThresholdExceeded() throws InterruptedException {
        ProductionLine self = new ProductionLine(1, "127.0.0.1", 5001);
        ProductionLineNode node = new ProductionLineNode(self, "http://localhost:8080");

        assertEquals(OperationalState.FULLY_OPERATIONAL, node.getState());

        // Start monitoring
        node.startMonitoring();

        // Pause the physical sensor so it doesn't write noise measurements concurrently
        node.getSensor().pauseMeasuring();
        node.getBuffer().clear();

        // Feed 8 measurements with values > 80.0 to trigger the threshold
        long now = System.currentTimeMillis();
        for (int i = 0; i < 8; i++) {
            node.getBuffer().addMeasurement(new sensor.Measurement("Vibration-1", "Vibration", 90.0, now + i));
        }

        // Give the background monitoring thread a moment to consume and process the window
        Thread.sleep(300);

        // Assert that the state transitioned to WAITING_FOR_CALIBRATION
        assertEquals(OperationalState.WAITING_FOR_CALIBRATION, node.getState());

        // Cleanup
        node.stopMonitoring();
    }

    @Test
    public void testLogicalClockUpdates() {
        ProductionLine self = new ProductionLine(1, "127.0.0.1", 5001);
        ProductionLineNode node = new ProductionLineNode(self, "http://localhost:8080");

        assertEquals(0, node.getLogicalClock());

        node.incrementClock();
        assertEquals(1, node.getLogicalClock());

        // Update clock on receive: max(1, 10) + 1 = 11
        node.updateClockOnReceive(10);
        assertEquals(11, node.getLogicalClock());

        // Update clock on receive: max(11, 5) + 1 = 12
        node.updateClockOnReceive(5);
        assertEquals(12, node.getLogicalClock());
    }

    @Test
    public void testCalibrationRequestPriorityHandling() {
        ProductionLine self = new ProductionLine(2, "127.0.0.1", 5002);
        ProductionLineNode node = new ProductionLineNode(self, "http://localhost:8080");
        PeerServiceImpl service = new PeerServiceImpl(node);

        // Case 1: Node is FULLY_OPERATIONAL. Should reply immediately.
        node.setState(OperationalState.FULLY_OPERATIONAL);

        final AtomicBoolean replied1 = new AtomicBoolean(false);
        StreamObserver<CalibrationReply> observer1 = new StreamObserver<>() {
            @Override public void onNext(CalibrationReply value) { replied1.set(true); }
            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        };

        CalibrationRequest request1 = CalibrationRequest.newBuilder()
                .setSenderId(1)
                .setCriticality(0.5)
                .setTimestamp(5)
                .build();

        service.requestCalibration(request1, observer1);
        assertTrue(replied1.get(), "Should reply immediately when FULLY_OPERATIONAL");

        // Case 2: Node is UNDER_CALIBRATION. Should defer.
        node.setState(OperationalState.UNDER_CALIBRATION);
        final AtomicBoolean replied2 = new AtomicBoolean(false);
        StreamObserver<CalibrationReply> observer2 = new StreamObserver<>() {
            @Override public void onNext(CalibrationReply value) { replied2.set(true); }
            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        };
        service.requestCalibration(request1, observer2);
        assertFalse(replied2.get(), "Should defer reply when UNDER_CALIBRATION");
        assertEquals(1, node.getAndClearDeferredObservers().size());

        // Case 3: Node is WAITING_FOR_CALIBRATION.
        // Local node (ID 2): average = 90 (criticality = (90-80)/80 = 0.125)
        node.setState(OperationalState.WAITING_FOR_CALIBRATION);
        node.setLastCalculatedAverage(90.0);

        // Subcase 3a: Sender (ID 3) has higher criticality (0.5 > 0.125). Node 2 should reply immediately.
        final AtomicBoolean replied3a = new AtomicBoolean(false);
        StreamObserver<CalibrationReply> observer3a = new StreamObserver<>() {
            @Override public void onNext(CalibrationReply value) { replied3a.set(true); }
            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        };
        CalibrationRequest request3a = CalibrationRequest.newBuilder()
                .setSenderId(3)
                .setCriticality(0.5)
                .setTimestamp(5)
                .build();
        service.requestCalibration(request3a, observer3a);
        assertTrue(replied3a.get(), "Should reply immediately when sender has higher criticality");

        // Subcase 3b: Sender (ID 1) has lower criticality (0.05 < 0.125). Node 2 should defer.
        final AtomicBoolean replied3b = new AtomicBoolean(false);
        StreamObserver<CalibrationReply> observer3b = new StreamObserver<>() {
            @Override public void onNext(CalibrationReply value) { replied3b.set(true); }
            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        };
        CalibrationRequest request3b = CalibrationRequest.newBuilder()
                .setSenderId(1)
                .setCriticality(0.05)
                .setTimestamp(5)
                .build();
        service.requestCalibration(request3b, observer3b);
        assertFalse(replied3b.get(), "Should defer when local criticality is higher");

        // Subcase 3c: Same criticality (0.125 == 0.125). Tie-breaker on highest ID.
        // Node 2 has ID 2. Sender has ID 1. Node 2 has higher ID (highest priority), so Node 2 should defer.
        final AtomicBoolean replied3c = new AtomicBoolean(false);
        StreamObserver<CalibrationReply> observer3c = new StreamObserver<>() {
            @Override public void onNext(CalibrationReply value) { replied3c.set(true); }
            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        };
        CalibrationRequest request3c = CalibrationRequest.newBuilder()
                .setSenderId(1)
                .setCriticality(0.125)
                .setTimestamp(5)
                .build();
        service.requestCalibration(request3c, observer3c);
        assertFalse(replied3c.get(), "Should defer when local ID is higher (criticality tie-breaker)");

        // Subcase 3d: Same criticality (0.125 == 0.125).
        // Node 2 has ID 2. Sender has ID 3. Sender has higher ID (highest priority), so Node 2 should reply immediately.
        final AtomicBoolean replied3d = new AtomicBoolean(false);
        StreamObserver<CalibrationReply> observer3d = new StreamObserver<>() {
            @Override public void onNext(CalibrationReply value) { replied3d.set(true); }
            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        };
        CalibrationRequest request3d = CalibrationRequest.newBuilder()
                .setSenderId(3)
                .setCriticality(0.125)
                .setTimestamp(5)
                .build();
        service.requestCalibration(request3d, observer3d);
        assertTrue(replied3d.get(), "Should reply immediately when sender has higher ID (criticality tie-breaker)");
    }

    @Test
    public void testRequestCalibrationClientBroadcast() {
        ProductionLine self = new ProductionLine(1, "127.0.0.1", 5001);
        ProductionLineNode node = new ProductionLineNode(self, "http://localhost:8080");

        // Add a mock peer
        ProductionLine peer = new ProductionLine(2, "127.0.0.1", 5002);
        node.addPeer(peer);

        assertEquals(0, node.getLogicalClock());
        assertEquals(0, node.getRepliesReceived());

        // Perform request broadcast
        node.requestCalibration(90.0);

        // Assert logical clock incremented
        assertEquals(1, node.getLogicalClock());

        // Since the peer is offline, the exception is caught, and it fallback-increments the reply counter
        assertEquals(1, node.getRepliesReceived());
    }
}
