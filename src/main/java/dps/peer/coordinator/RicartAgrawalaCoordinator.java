package dps.peer.coordinator;

import dps.common.model.OperationalState;
import dps.common.model.ProductionLine;
import dps.peer.ProductionLineNode;
import dps.peer.proto.CalibrationReply;
import dps.peer.proto.CalibrationRequest;
import dps.peer.proto.PeerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Coordinates the Ricart-Agrawala mutual exclusion algorithm for calibration access.
 * Tracks logical clocks, replies, and deferred observers, and manages access requests.
 */
public class RicartAgrawalaCoordinator {

    private final ProductionLineNode node;
    private final ProductionLine self;
    
    // Ricart-Agrawala variables
    private long logicalClock = 0;
    private long requestTimestamp = 0;
    private double requestCriticality = 0.0;
    private final Map<Integer, StreamObserver<CalibrationReply>> deferredObservers = new HashMap<>();   // deferred response queue (pending gRPC StreamObserver)
    private double lastCalculatedAverage = 0.0;
    
    // Reply tracking by peer ID
    private final Set<Integer> repliesReceived = new HashSet<>();

    public RicartAgrawalaCoordinator(ProductionLineNode node, ProductionLine self) {
        if (node == null) {
            throw new IllegalArgumentException("ProductionLineNode reference cannot be null.");
        }
        this.node = node;
        this.self = self;
    }

    // Thread-safe Lamport clock and RA helper methods
    public synchronized long getLogicalClock() {
        return logicalClock;
    }

    public synchronized void incrementClock() {
        logicalClock++;
    }

    public synchronized void updateClockOnReceive(long receivedTime) {
        logicalClock = Math.max(logicalClock, receivedTime) + 1;
    }

    public synchronized void addDeferredObserver(int peerId, StreamObserver<CalibrationReply> observer, String reason) {
        deferredObservers.put(peerId, observer);
        System.out.println("[PEER " + self.id() + "] [" + node.getState() + "] Deferring reply to Node " + peerId + " - Reason: " + reason);
    }

    public synchronized void addDeferredObserver(int peerId, StreamObserver<CalibrationReply> observer) {
        addDeferredObserver(peerId, observer, "none");
    }

    public synchronized List<StreamObserver<CalibrationReply>> getAndClearDeferredObservers() {
        List<StreamObserver<CalibrationReply>> observers = new ArrayList<>(deferredObservers.values());
        deferredObservers.clear();
        return observers;
    }

    public synchronized double getLastCalculatedAverage() {
        return lastCalculatedAverage;
    }

    public synchronized void setLastCalculatedAverage(double average) {
        this.lastCalculatedAverage = average;
    }

    // Reply tracking methods
    public synchronized void addReply(int peerId) {
        repliesReceived.add(peerId);
        notifyAll(); // Wake up thread waiting for replies
    }

    public synchronized void removeReply(int peerId) {
        repliesReceived.remove(peerId);
        notifyAll(); // Wake up thread in case count drops
    }

    public synchronized boolean hasReplyFrom(int peerId) {
        return repliesReceived.contains(peerId);
    }

    public synchronized void incrementRepliesReceived() {
        addReply(-repliesReceived.size() - 1); // backward compatibility helper
    }

    public synchronized int getRepliesReceived() {
        return repliesReceived.size();
    }

    public synchronized void resetRepliesReceived() {
        repliesReceived.clear();
    }

    public synchronized double calcCriticality() {
        double threshold = node.getVibrationThreshold();
        return (lastCalculatedAverage - threshold) / threshold;
    }

    /**
     * Broadcasts a calibration request to all peers.
     */
    public void requestCalibration() {
        List<ProductionLine> currentPeers = node.getNetworkManager().getPeers(); // delegated to NetworkManager

        synchronized (this) {
            incrementClock(); // send event -> increment Lamport's clock
            requestTimestamp = getLogicalClock(); // memorize current clock value
            requestCriticality = calcCriticality();  // calculate criticality based on threshold
            resetRepliesReceived(); // reset replies counter to start new round
        }

        if (currentPeers.isEmpty()) {
            System.out.println("[PEER " + self.id() + "] [" + node.getState() + "] No peers in topology. No replies needed.");
            return;
        }

        System.out.println("[PEER " + self.id() + "] [" + node.getState() + "] Requesting calibration from " + currentPeers.size() 
                + " peer(s) in parallel (Clock: " + requestTimestamp 
                + ", Criticality: " + String.format("%.4f", requestCriticality) + ")...");

        for (ProductionLine peer : currentPeers) {
            sendCalibrationRequestToPeerAsynchronously(peer);
        }
    }

    /**
     * Sends a calibration request to a single target peer asynchronously.
     */
    public void sendCalibrationRequestToPeerAsynchronously(ProductionLine peer) {
        new Thread(() -> {
            sendCalibrationRequestToPeer(peer);
        }, "Re-Request-Thread-Node-" + self.id() + "-to-" + peer.id()).start();
    }

    /**
     * Executes the blocking gRPC call to request calibration from a single peer.
     */
    public void sendCalibrationRequestToPeer(ProductionLine peer) {
        double reqCriticality;
        long reqTimestamp;
        synchronized (this) {
            reqCriticality = this.requestCriticality;
            reqTimestamp = this.requestTimestamp;
        }

        ManagedChannel channel = null;
        try {
            channel = ManagedChannelBuilder.forAddress(peer.ip(), peer.port())
                    .usePlaintext()
                    .build();

            PeerServiceGrpc.PeerServiceBlockingStub stub = PeerServiceGrpc.newBlockingStub(channel);

            CalibrationRequest request = CalibrationRequest.newBuilder()
                    .setSenderId(self.id())
                    .setCriticality(reqCriticality)
                    .setTimestamp(reqTimestamp)
                    .build();

            /*
             * Send request with a 120-second (2-minute) timeout.
             * Why 120 seconds:
             * 1) Queue Accumulation: When multiple nodes (e.g., 15+) want to calibrate concurrently,
             *    their calibration times (up to 7 seconds each) accumulate in the queue. A 120-second
             *    deadline ensures healthy waiting nodes do not time out prematurely, preserving mutual exclusion.
             * 2) Crash Handling (Section 10.2): If a peer process crashes, the OS closes the TCP socket immediately.
             *    gRPC detects this instantly (throwing UNAVAILABLE in milliseconds), meaning we don't wait 120s
             *    to recover from a standard process crash.
             */
            stub.withDeadlineAfter(120, TimeUnit.SECONDS).requestCalibration(request);

            // Reply received successfully
            addReply(peer.id());
            System.out.println("[PEER " + self.id() + "] [" + node.getState() + "] Received CalibrationReply from Node " + peer.id());

        } catch (Exception e) {
            System.err.println("[PEER " + self.id() + "] [" + node.getState() + "] ❌ Failed to get CalibrationReply from Node " 
                    + peer.id() + " - Error: " + e.getMessage());
            // In case of communication failure or timeout, treat as implicit reply to avoid deadlocks
            addReply(peer.id());
        } finally {
            if (channel != null) {
                try {
                    channel.shutdown().awaitTermination(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * Blocks the current thread and requests calibration access from peers.
     * Transitions state to UNDER_CALIBRATION and performs the simulated calibration once allowed.
     */
    public void enterCalibrationAndWait() {
        // The method is not entirely synchronized, otherwise we would block all threads for the entire duration of the calibration or the network communications
        // so the synchronization is implemented at a finer granularity.

        double avg = getLastCalculatedAverage();
        
        System.out.println("[PEER " + self.id() + "] [WAITING_FOR_CALIBRATION] Initiating Ricart-Agrawala calibration sequence...");
        
        // 1. Broadcast the requests to peers
        requestCalibration();

        // 2. Wait until we receive all replies
        int requiredReplies = node.getNetworkManager().getPeerCount();
        synchronized (this) {
            while (getRepliesReceived() < requiredReplies) {
                try {
                    System.out.println("[PEER " + self.id() + "] [" + node.getState() + "] Waiting for replies... (Progress: " 
                            + getRepliesReceived() + "/" + requiredReplies + ")");
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            
            // 3. Enter calibration section
            node.setState(OperationalState.UNDER_CALIBRATION);
        }

        // Generate random calibration duration between 3 and 7 seconds
        long duration = 3000 + (long) (Math.random() * 4000);
        System.out.println("[PEER " + self.id() + "] [UNDER_CALIBRATION] 🛠️ Entered calibration mode. Calibrating for " + duration + " ms...");
        
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("[PEER " + self.id() + "] [UNDER_CALIBRATION] Calibration execution finished.");
    }

    /**
     * Releases the calibration resource, replies to all deferred peer requests,
     * updates the state to FULLY_OPERATIONAL, and restarts the physical sensor.
     */
    public void releaseCalibration() {
        List<StreamObserver<CalibrationReply>> observers;

        synchronized (this) {
            node.setState(OperationalState.FULLY_OPERATIONAL);
            observers = getAndClearDeferredObservers();     // get the observers accumulated when calibration was locked by this
        }

        System.out.println("[PEER " + self.id() + "] [FULLY_OPERATIONAL] Calibration completed. Releasing " 
                + observers.size() + " deferred replies...");

        for (StreamObserver<CalibrationReply> observer : observers) {       // iterate on the observers
            try {
                observer.onNext(CalibrationReply.getDefaultInstance());     // send consensus reply
                observer.onCompleted();     // close connection with peer (client)
            } catch (Exception e) {
                System.err.println("[PEER " + self.id() + "] [" + node.getState() + "] ❌ Failed to send deferred reply to peer - Error: " + e.getMessage());
            }
        }

        // Restart physical sensor measuring loop
        node.getSensorManager().getSensor().startMeasuring();
        System.out.println("[PEER " + self.id() + "] [" + node.getState() + "] Physical sensor simulator resumed.");
    }
}
