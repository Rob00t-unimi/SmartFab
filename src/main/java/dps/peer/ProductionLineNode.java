package dps.peer;

import dps.common.model.ProductionLine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProductionLineNode {

    public record NodeConfig(ProductionLine self, String serverUrl) {}

    // Map of active peers, manually synchronized on 'this'
    private final Map<Integer, ProductionLine> peers = new HashMap<>();

    private final ProductionLine self;
    private final String serverUrl;

    public ProductionLineNode(ProductionLine self, String serverUrl) {
        if (self == null) {
            throw new IllegalArgumentException("ProductionLine identity cannot be null.");
        }
        this.self = self;
        this.serverUrl = serverUrl;
    }

    public static void main(String[] args) {
        try {
            NodeConfig config = parseArgs(args);
            ProductionLineNode node = new ProductionLineNode(config.self(), config.serverUrl());

            System.out.println("Starting Production Line Node " + node.getSelf().id() + " on " + node.getSelf().ip() + ":" + node.getSelf().port());
            System.out.println("Admin Server URL: " + node.getServerUrl());

            // REST Registration and gRPC initialization will be added in subsequent steps.

        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            System.err.println("Usage: java dps.peer.ProductionLineNode <id> <ip> <port> [serverUrl]");
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
