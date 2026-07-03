package dps.peer.net;

import dps.common.model.ProductionLine;
import dps.peer.ProductionLineNode;
import dps.peer.proto.NodeIdentity;
import dps.peer.proto.PeerServiceGrpc;
import dps.peer.proto.PresentationRequest;
import dps.peer.proto.PresentationResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Handles all REST (bootstrap/registration) and P2P gRPC (lifecycle, presentation)
 * network communications for the Production Line node.
 */
public class NetworkManager {

    private final ProductionLineNode node;
    private final ProductionLine self;
    private final String serverUrl;

    // Map of active peers, manually synchronized on 'this'
    private final Map<Integer, ProductionLine> peers = new HashMap<>();

    private Server grpcServer;

    public NetworkManager(ProductionLineNode node, ProductionLine self, String serverUrl) {
        if (node == null) {
            throw new IllegalArgumentException("ProductionLineNode reference cannot be null.");
        }
        this.node = node;
        this.self = self;
        this.serverUrl = serverUrl;
    }

    // Thread-safe peer management using basic synchronized blocks
    public synchronized void addPeer(ProductionLine peer) {
        if (peer == null) {
            throw new IllegalArgumentException("Peer cannot be null.");
        }
        peers.put(peer.id(), peer);
    }

    public synchronized void removePeer(int peerId) {
        peers.remove(peerId);
    }

    public synchronized List<ProductionLine> getPeers() {
        return new ArrayList<>(peers.values());
    }

    public synchronized int getPeerCount() {
        return peers.size();
    }

    public synchronized ProductionLine getPeerById(int id) {
        return peers.get(id);
    }

    /**
     * Starts the local gRPC Server on the configured port.
     */
    public synchronized void startGrpcServer() throws IOException {
        if (grpcServer != null) {
            return;
        }

        grpcServer = ServerBuilder.forPort(self.port())
                .addService(new PeerServiceImpl(node))
                .build()
                .start();

        System.out.println("[PEER " + self.id() + "] gRPC server started, listening on port " + self.port());

        // Add a shutdown hook to stop the gRPC server when JVM shuts down
        // starts a new thread that performs cleanup during the shutdown phase
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[PEER " + self.id() + "] Shutdown hook triggered. Stopping gRPC server and monitoring...");
            NetworkManager.this.stopGrpcServer();
            node.stopMonitoring();
        }));
    }

    /**
     * Gracefully stops the local gRPC Server.
     */
    public synchronized void stopGrpcServer() {
        if (grpcServer != null) {
            grpcServer.shutdown();
            try {
                if (!grpcServer.awaitTermination(3, TimeUnit.SECONDS)) {
                    grpcServer.shutdownNow();
                }
            } catch (InterruptedException e) {
                // If an interrupt occurs during the "try" phase, an exception is thrown and the interrupt must be re-raised.
                grpcServer.shutdownNow();
                Thread.currentThread().interrupt();
            }
            grpcServer = null;
            System.out.println("[PEER " + self.id() + "] gRPC server stopped.");
        }
    }

    /**
     * Blocks the thread until the gRPC server is terminated.
     */
    public void blockUntilGRPCShutdown() throws InterruptedException {
        if (grpcServer != null) {
            grpcServer.awaitTermination();
        }
    }

    /**
     * Registers this node with the Admin Server via REST POST.
     * Populates the local peer list with the response on success.
     */
    public void registerWithAdminServer() {
        RestTemplate restTemplate = new RestTemplate();
        String url = this.serverUrl + "/production-lines";

        try {
            // Perform HTTP POST request (the only post avaiable in /production-lines is "register").
            // Spring will automatically serialize 'this.self' into JSON
            // and deserialize the response JSON array into an array of ProductionLine.
            ProductionLine[] response = restTemplate.postForObject(url, this.self, ProductionLine[].class); // The third parameter tells Spring which object to deserialize the response into.
            if (response != null) {
                for (ProductionLine peer : response) {
                    addPeer(peer); // synchronized
                }
                System.out.println("[PEER " + self.id() + "] REST registration successful. Loaded " + response.length + " peer(s) from Admin Server.");
            }
        } catch (HttpClientErrorException.Conflict e) {
            throw new IllegalStateException("Registration conflict: Node with ID " + self.id() + " is already registered.");
        } catch (ResourceAccessException e) {
            throw new IllegalStateException("Connection failed: Admin Server is offline or unreachable at " + url);
        } catch (Exception e) {
            throw new IllegalStateException("Registration failed due to unexpected error: " + e.getMessage(), e);
        }
    }

    /**
     * Broadcasts a gRPC presentation request in parallel to all currently known peers.
     */
    public void presentSelfToPeers() {
        List<ProductionLine> currentPeers = getPeers(); // synchronized
        if (currentPeers.isEmpty()) {
            System.out.println("[PEER " + self.id() + "] No existing peers to present to in local network view.");
            return;
        }

        System.out.println("[PEER " + self.id() + "] Sending gRPC presentation requests to " + currentPeers.size() + " peer(s) in parallel...");

        // P2P broadcasts must be done in parallel by using CachedThreadPool.
        ExecutorService executor = Executors.newCachedThreadPool();

        for (ProductionLine peer : currentPeers) {
            executor.submit(() -> {  // For each peer, we assign and execute a lambda to a thread into the pool
                // Tasks are assigned to the pool threads sequentially within the loop, but the threads execute the gRPC calls in parallel, without waiting for the previous thread to establish or complete its connection.
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forAddress(peer.ip(), peer.port())  // gRPC channel creation
                            .usePlaintext() // http instead of https
                            .build();

                    // Creating a stub object locally allows server remote procedure calls to be made from it.
                    PeerServiceGrpc.PeerServiceBlockingStub stub = PeerServiceGrpc.newBlockingStub(channel); // BlockingStub, because the current thread waits for the RPC response.

                    // Constructing Messages using the Builder Pattern auto-generated by Protobuf
                    NodeIdentity identity = NodeIdentity.newBuilder()   // NodeIdentity (object generated by Protobuf)
                            .setId(self.id())
                            .setIp(self.ip())
                            .setPort(self.port())
                            .build();

                    // Construction of the request
                    PresentationRequest request = PresentationRequest.newBuilder()  // PresentationRequest (object generated by Protobuf)
                            .setSender(identity)
                            .build();

                    // Executing the gRPC call with a 3-second timeout to a gRPC server of another peer
                    PresentationResponse response = stub.withDeadlineAfter(3, TimeUnit.SECONDS).present(request);
                    if (response.getAccepted()) {
                        System.out.println("[PEER " + self.id() + "] [" + node.getState() + "] gRPC presentation ACCEPTED by Node " + peer.id());
                    } else {
                        System.out.println("[PEER " + self.id() + "] [" + node.getState() + "] ⚠️ gRPC presentation REJECTED by Node " + peer.id());
                    }

                } catch (Exception e) {
                    System.err.println("[PEER " + self.id() + "] [" + node.getState() + "] ❌ gRPC presentation FAILED to Node " + peer.id() + " - Error: " + e.getMessage());
                } finally {
                    if (channel != null) {
                        try {
                            // close the gRPC channel
                            channel.shutdown().awaitTermination(1, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            });
        }

        // Shutdown Thread Pool
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt(); // If an interrupt arrives that sends execution to the "catch" block, I re-throw it.
        }
    }
}
