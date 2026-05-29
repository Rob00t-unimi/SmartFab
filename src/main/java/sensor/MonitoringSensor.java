package sensor;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class MonitoringSensor extends Simulator {
    private static int ID = 1;

    private double t = 0;

    private double k;
    private double amp;
    private double tInflection;

    public MonitoringSensor(Buffer buffer) {
        super("Vibration-" + (ID++), "Vibration", buffer, 375);

        generateParameters();
    }

    private void generateParameters() {
        this.k = 0.10 + rnd.nextDouble() * 0.90;
        this.amp = 70.0 + rnd.nextDouble() * 60.0;
        // this.tInflection = 45.0 + rnd.nextDouble() * 20.0;
        this.tInflection = 30.0 + rnd.nextDouble() * 60.0;
    }

    @Override
    protected void generateMeasurement() {
        addMeasurement(getVibrationValue(t));
        t += 1;
    }

    @Override
    protected void onResume() {
        generateParameters();
        t = 0;
    }

    double getVibrationValue(double t) {
        double trend = 45.0 + amp / (1.0 + Math.exp(-k * (t - tInflection)));
        double wave  =  1.5 * Math.sin(t * 0.3);
        double noise = rnd.nextGaussian() * 0.5;
        return Math.max(0.0, trend + wave + noise);
    }
}
