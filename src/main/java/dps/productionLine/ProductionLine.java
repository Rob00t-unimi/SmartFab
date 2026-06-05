package dps.productionLine;

import java.util.regex.Pattern;

/**
 * Shared model representing a production line identity in the network.
 */
public record ProductionLine(int id, String ip, int port) {
    private static final Pattern IP_PATTERN = Pattern.compile("^((25[0-5]|(2[0-4]|1\\d|[1-9]|)\\d)\\.?\\b){4}$");

    public ProductionLine {
        if (id < 0) throw new IllegalArgumentException("ID must be non-negative");
        if (ip == null || ip.isBlank()) throw new IllegalArgumentException("IP cannot be null or empty");
        if (!IP_PATTERN.matcher(ip).matches() && !ip.equals("localhost")) throw new IllegalArgumentException("Invalid IP address format");
        if (port < 1024 || port > 65535) throw new IllegalArgumentException("Port must be between 1024 and 65535");
    }
}
