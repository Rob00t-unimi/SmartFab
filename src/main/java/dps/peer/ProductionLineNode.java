package dps.peer;

import dps.common.model.ProductionLine;

public class ProductionLineNode {

    public record NodeConfig(ProductionLine self, String serverUrl) {}

    public static void main(String[] args) {
        try {
            NodeConfig config = parseArgs(args);
            System.out.println("Starting Production Line Node " + config.self().id() + " on " + config.self().ip() + ":" + config.self().port());
            System.out.println("Admin Server URL: " + config.serverUrl());

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
}
