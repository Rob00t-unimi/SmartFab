package sensor;

public record Measurement(String id, String type, double value, long timestamp) implements Comparable<Measurement> {
    @Override
    public int compareTo(Measurement m) {
        Long thisTimestamp = timestamp;
        Long otherTimestamp = m.timestamp();
        return thisTimestamp.compareTo(otherTimestamp);
    }

    public String toString(){
        return value + " " + timestamp;
    }
}
