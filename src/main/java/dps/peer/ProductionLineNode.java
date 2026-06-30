package dps.peer;

import dps.common.model.ProductionLine;
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
import java.util.concurrent.TimeUnit;

public class ProductionLineNode {

    public record NodeConfig(ProductionLine self, String serverUrl) {}

    // Map of active peers, manually synchronized on 'this'
    private final Map<Integer, ProductionLine> peers = new HashMap<>();

    private final ProductionLine self;
    private final String serverUrl;
    
    private Server grpcServer;

    public ProductionLineNode(ProductionLine self, String serverUrl) {
        if (self == null) {
            throw new IllegalArgumentException("ProductionLine identity cannot be null.");
        }
        this.self = self;
        this.serverUrl = serverUrl;
    }

    public static void main(String[] args) {
        ProductionLineNode node = null;
        try {
            NodeConfig config = parseArgs(args);
            node = new ProductionLineNode(config.self(), config.serverUrl());

            System.out.println("Starting Production Line Node " + node.getSelf().id() + " on " + node.getSelf().ip() + ":" + node.getSelf().port());
            System.out.println("Admin Server URL: " + node.getServerUrl());

            // 1. Start gRPC Server first so that peers can reach us as soon as we register
            node.startGrpcServer();

            // 2. REST Registration
            node.registerWithAdminServer();

            System.out.println("Node " + node.getSelf().id() + " is running. Press Ctrl+C to exit.");
            
            // Block until JVM is terminated
            node.blockUntilShutdown();

        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            System.err.println("Usage: java dps.peer.ProductionLineNode <id> <ip> <port> [serverUrl]");
            System.exit(1);
        } catch (IllegalStateException e) {
            System.err.println("Startup Failed: " + e.getMessage());
            if (node != null) {
                node.stopGrpcServer();
            }
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Unexpected Error: " + e.getMessage());
            if (node != null) {
                node.stopGrpcServer();
            }
            System.exit(1);
        }
    }

    public static NodeConfig parseArgs(String[] args) {
        if (args == null || args.length < 3) {
            throw new IllegalArgumentException("Insufficient arguments. ID, IP, and Port are required.");
        }

        int id;
        try {
            id = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("ID must be an integer.");
        }

        String ip = args[1];

        int port;
        try {
            port = Integer.parseInt(args[2]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Port must be an integer.");
        }

        String serverUrl = args.length >= 4 ? args[3] : "http://localhost:8080";

        // This will perform the internal validations of the ProductionLine record (IP format, port range, etc.)
        ProductionLine self = new ProductionLine(id, ip, port);

        return new NodeConfig(self, serverUrl);
    }

    /**
     * Starts the local gRPC Server on the configured port.
     */
    public synchronized void startGrpcServer() throws IOException {
        if (grpcServer != null) {
            return;
        }

        grpcServer = ServerBuilder.forPort(self.port())
                .addService(new PeerServiceImpl(this))
                .build()
                .start();

        System.out.println("Node " + self.id() + ": gRPC server started, listening on port " + self.port());

        // Add a shutdown hook to stop the gRPC server when JVM shuts down
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown hook triggered for node " + self.id() + ". Stopping gRPC server...");
            ProductionLineNode.this.stopGrpcServer();
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
                grpcServer.shutdownNow();
                Thread.currentThread().interrupt();
            }
            grpcServer = null;
            System.out.println("Node " + self.id() + ": gRPC server stopped.");
        }
    }

    /**
     * Blocks the thread until the gRPC server is terminated.
     */
    public void blockUntilShutdown() throws InterruptedException {
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
            // Perform HTTP POST request. Spring will automatically serialize 'this.self' into JSON
            // and deserialize the response JSON array into an array of ProductionLine.
            ProductionLine[] response = restTemplate.postForObject(url, this.self, ProductionLine[].class);
            if (response != null) {
                for (ProductionLine peer : response) {
                    addPeer(peer);
                }
                System.out.println("Node " + self.id() + " registered successfully. Loaded " + response.length + " peer(s) from registry.");
            }
        } catch (HttpClientErrorException.Conflict e) {
            throw new IllegalStateException("Registration conflict: Node with ID " + self.id() + " is already registered.");
        } catch (ResourceAccessException e) {
            throw new IllegalStateException("Connection failed: Admin Server is offline or unreachable at " + url);
        } catch (Exception e) {
            throw new IllegalStateException("Registration failed due to unexpected error: " + e.getMessage(), e);
        }
    }

    public ProductionLine getSelf() {
        return self;
    }

    public String getServerUrl() {
        return serverUrl;
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
}
