package dps.adminServer;

import dps.common.model.OperationalState;
import dps.common.model.ProductionLine;
import dps.common.model.ProductionLineStatus;

import java.util.*;

/**
 * Registry for production lines with fine-grained synchronization.
 * It uses an internal class to manage data per production line, allowing
 * concurrent access to different lines.
 */
public class ProductionLineRegistry {

    // Record for telemetry entries
    public record TelemetryEntry(double average, long timestamp) {}

    /**
     * Internal class to hold data for a single production line.
     * Provides its own synchronization to allow fine-grained locking.
     */
    private static class ProductionLineData {
        private final ProductionLine line;
        private OperationalState state;
        private final List<TelemetryEntry> telemetry = new ArrayList<>();

        public ProductionLineData(ProductionLine line) {
            this.line = line;
            this.state = OperationalState.FULLY_OPERATIONAL;
        }

        public synchronized void updateState(OperationalState state) {
            this.state = state;
        }

        public synchronized OperationalState getState() {
            return state;
        }

        public synchronized void addTelemetry(double average, long timestamp) {
            telemetry.add(new TelemetryEntry(average, timestamp));
        }

        public synchronized List<TelemetryEntry> getTelemetrySnapshot() {
            // Returns a copy to allow processing outside of the lock
            return new ArrayList<>(telemetry);
        }

        public ProductionLine getLine() {
            return line;
        }
    }

    // Global map: access to this map must be synchronized on 'this'
    private final Map<Integer, ProductionLineData> lineDataMap = new HashMap<>();

    /**
     * Registers a new production line.
     * Synchronized on 'this' because it modifies the shared map structure.
     */
    public synchronized List<ProductionLine> register(ProductionLine newLine) {
        if (lineDataMap.containsKey(newLine.id())) {
            System.err.println("[ADMIN_SERVER] Registration CONFLICT: Node with ID " + newLine.id() + " is already registered.");
            throw new IllegalArgumentException("Production Line with ID " + newLine.id() + " is already registered.");
        }
        
        // Prepare current peers list
        List<ProductionLine> peers = new ArrayList<>();
        for (ProductionLineData data : lineDataMap.values()) {
            peers.add(data.getLine());
        }

        // Add the new line
        lineDataMap.put(newLine.id(), new ProductionLineData(newLine));
        
        System.out.println("[ADMIN_SERVER] Registered new Production Line: ID " + newLine.id() + " on " + newLine.ip() + ":" + newLine.port());
        System.out.println("[ADMIN_SERVER] Returning list of " + peers.size() + " active peer(s) to Node " + newLine.id());
        
        return peers;
    }

    /**
     * Returns a list of all production lines with their current states.
     * Minimizes the global lock duration.
     */
    public List<ProductionLineStatus> getLinesStatus() {
        List<ProductionLineData> dataSnapshot;
        synchronized (this) {
            // Take a quick snapshot of the map values (references)
            dataSnapshot = new ArrayList<>(lineDataMap.values());
        }
        
        List<ProductionLineStatus> statusList = new ArrayList<>();
        for (ProductionLineData data : dataSnapshot) {
            // getState() is synchronized on the individual line object, not on 'this'
            statusList.add(new ProductionLineStatus(data.getLine(), data.getState()));
        }
        return statusList;
    }

    /**
     * Updates the state of a specific line.
     */
    public void updateState(int id, OperationalState state) {
        ProductionLineData data;
        synchronized (this) {
            data = lineDataMap.get(id);
        }
        if (data != null) {
            // Locking only the specific line
            data.updateState(state);
            System.out.println("[ADMIN_SERVER] Updated state for Node " + id + " to: " + state);
        } else {
            System.err.println("[ADMIN_SERVER] State update failed: Node " + id + " not found in registry.");
        }
    }

    /**
     * Adds telemetry data to a specific line.
     */
    public void addTelemetry(int id, double average, long timestamp) {
        ProductionLineData data;
        synchronized (this) {
            data = lineDataMap.get(id);
        }
        if (data != null) {
            // Locking only the specific line
            data.addTelemetry(average, timestamp);
            System.out.println("[ADMIN_SERVER] Added telemetry for Node " + id + ": average vibration = " + average);
        } else {
            System.err.println("[ADMIN_SERVER] Telemetry update failed: Node " + id + " not found in registry.");
        }
    }

    /**
     * Computes average vibration for a line between t1 and t2.
     * Fine-grained: locks the registry briefly to find the line, 
     * then locks the line to get a snapshot, then computes WITHOUT lock.
     */
    public double getAverageVibration(int id, long t1, long t2) {
        ProductionLineData data;
        synchronized (this) {
            data = lineDataMap.get(id);
        }

        if (data == null) {
            System.err.println("[ADMIN_SERVER] Stats query failed: Node " + id + " not found.");
            throw new NoSuchElementException("Production Line with ID " + id + " not found.");
        }

        // Get a snapshot of telemetry (synchronized on 'data')
        List<TelemetryEntry> entries = data.getTelemetrySnapshot();

        // Computation happens WITHOUT holding any lock, allowing other threads to work!
        double sum = 0;
        int count = 0;
        for (TelemetryEntry entry : entries) {
            if (entry.timestamp() >= t1 && entry.timestamp() <= t2) {
                sum += entry.average();
                count++;
            }
        }
        double avg = count == 0 ? 0.0 : sum / count;
        
        System.out.println("[ADMIN_SERVER] Stats queried for Node " + id + " between " + t1 + " and " + t2 
                + ". Found " + count + " entries. Average: " + avg);
        
        return avg;
    }

    /**
     * Helper for backward compatibility or direct list access.
     */
    public synchronized List<ProductionLine> getAllLines() {
        List<ProductionLine> lines = new ArrayList<>();
        for (ProductionLineData data : lineDataMap.values()) {
            lines.add(data.getLine());
        }
        return lines;
    }
}
