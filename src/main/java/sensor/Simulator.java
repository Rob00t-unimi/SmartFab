package sensor;

import java.util.Random;

public abstract class Simulator extends Thread {
    private static final long DEFAULT_SLEEP_INTERVAL_MS = 250;

    protected Random rnd = new Random();

    private final Buffer buffer;
    private final String id;
    private final String type;
    private final long sleepInterval;

    protected volatile boolean stopCondition = false;
    private volatile boolean paused = true;
    private final Object pauseLock = new Object();

    private long pauseGeneration = 0;
    private long lastObservedPauseGeneration = 0;

    public Simulator(String id, String type, Buffer buffer, long sleepInterval) {
        this.id = id;
        this.type = type;
        this.buffer = buffer;
        this.sleepInterval = sleepInterval;
    }

    public Simulator(String id, String type, Buffer buffer) {
        this(id, type, buffer, DEFAULT_SLEEP_INTERVAL_MS);
    }

    public String getID() {
        return id;
    }

    public Buffer getBuffer() {
        return buffer;
    }

    public final void startMeasuring() {
        synchronized (pauseLock) {
            paused = false;
            pauseLock.notifyAll();
        }

        if (getState() == State.NEW)
            start();
    }

    public final void pauseMeasuring() {
        synchronized (pauseLock) {
            paused = true;
            pauseGeneration++;
        }
    }

    public final void stopMeasuring() {
        stopCondition = true;

        synchronized (pauseLock) {
            paused = false;
            pauseLock.notifyAll();
        }
    }

    @Override
    public final void run() {
        while (!stopCondition) {
            waitForResume();
            if (stopCondition) break;
            generateMeasurement();
            sensorSleep();
        }
    }

    protected abstract void generateMeasurement();

    protected void onResume() {}

    protected final void addMeasurement(double value) {
        buffer.addMeasurement(new Measurement(id, type, value, System.currentTimeMillis()));
    }

    private void waitForResume() {
        long generationAtEntry;
        synchronized (pauseLock) {
            generationAtEntry = pauseGeneration;

            while (paused && !stopCondition) {
                try {
                    pauseLock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }

        if (generationAtEntry > lastObservedPauseGeneration && !stopCondition) {
            lastObservedPauseGeneration = generationAtEntry;
            onResume();
        }
    }

    private void sensorSleep() {
        try {
            Thread.sleep(sleepInterval);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
