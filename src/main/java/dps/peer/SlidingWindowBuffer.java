package dps.peer;

import sensor.Buffer;
import sensor.Measurement;

import java.util.List;

public class SlidingWindowBuffer implements Buffer {

    @Override
    public void addMeasurement(Measurement m) {
    }

    @Override
    public List<Measurement> readAllAndClear() {
        return List.of();
    }

    @Override
    public void clear() {

    }
}
