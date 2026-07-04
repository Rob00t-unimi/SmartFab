package dps.peer.config;

import dps.common.model.ProductionLine;

/**
 * Record holding the immutable configuration of a Peer Node,
 * parsed from standard command line arguments.
 */
public record NodeConfig(ProductionLine self, String serverUrl, String mqttBrokerUrl) {
    
    /**
     * Parses CLI arguments and returns a valid NodeConfig instance.
     * Throws IllegalArgumentException if arguments are invalid.
     */
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
            throw new IllegalArgumentException("Port must be an integer");
        }

        if (port < 1024 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1024 and 65535");
        }

        String serverUrl = args.length >= 4 ? args[3] : "http://localhost:8080";
        String mqttBrokerUrl = args.length >= 5 ? args[4] : "tcp://localhost:1883";

        // This will perform the internal validations of the ProductionLine record (IP format, port range, etc.)
        ProductionLine self = new ProductionLine(id, ip, port);

        return new NodeConfig(self, serverUrl, mqttBrokerUrl);
    }
}
