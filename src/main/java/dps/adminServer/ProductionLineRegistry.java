package dps.adminServer;

import dps.common.model.OperationalState;
import dps.common.model.ProductionLine;
import dps.common.model.ProductionLineStatus;

import java.util.*;

public class ProductionLineRegistry {
    private final Map<Integer, ProductionLine> lines = new HashMap<>();
    private final Map<Integer, OperationalState> states = new HashMap<>();
    private final Map<Integer, List<TelemetryEntry>> telemetryData = new HashMap<>();

    // Record for telemetry entries
    public record TelemetryEntry(double average, long timestamp) {}

    /**
     * Registers a new production line.
     * @return List of already registered peer lines.
     * @throws IllegalArgumentException if ID is already registered.
     */
    public synchronized List<ProductionLine> register(ProductionLine newLine) {
        if (lines.containsKey(newLine.id())) {
            throw new IllegalArgumentException("Production Line with ID " + newLine.id() + " is already registered.");
        }
        
        List<ProductionLine> peers = new ArrayList<>(lines.values());
        lines.put(newLine.id(), newLine);
        states.put(newLine.id(), OperationalState.FULLY_OPERATIONAL);
        telemetryData.put(newLine.id(), new ArrayList<>());
        
        return peers;
    }

    public synchronized List<ProductionLineStatus> getLinesStatus() {
        List<ProductionLineStatus> statusList = new ArrayList<>();
        for (Integer id : lines.keySet()) {
            statusList.add(new ProductionLineStatus(lines.get(id), states.get(id)));
        }
        return statusList;
    }

    public synchronized List<ProductionLine> getAllLines() {
        return new ArrayList<>(lines.values());
    }

    public synchronized void updateState(int id, OperationalState state) {
        if (states.containsKey(id)) {
            states.put(id, state);
        }
    }

    public synchronized Map<Integer, OperationalState> getCurrentStates() {
        return new HashMap<>(states);
    }

    public synchronized void addTelemetry(int id, double average, long timestamp) {
        if (telemetryData.containsKey(id)) {
            telemetryData.get(id).add(new TelemetryEntry(average, timestamp));
        }
    }

    /**
     * Computes average vibration for a line between t1 and t2.
     * Fine-grained synchronization: we only lock the telemetry list for that specific line.
     * @throws NoSuchElementException if the ID is not registered.
     */
    public double getAverageVibration(int id, long t1, long t2) {
        List<TelemetryEntry> entries;
        synchronized (this) {
            if (!lines.containsKey(id)) {
                throw new NoSuchElementException("Production Line with ID " + id + " not found.");
            }
            List<TelemetryEntry> original = telemetryData.get(id);
            if (original == null) return 0.0;
            // Create a copy to minimize the time we hold the main lock
            entries = new ArrayList<>(original);
        }

        double sum = 0;
        int count = 0;
        for (TelemetryEntry entry : entries) {
            if (entry.timestamp() >= t1 && entry.timestamp() <= t2) {
                sum += entry.average();
                count++;
            }
        }
        return count == 0 ? 0.0 : sum / count;
    }
}
