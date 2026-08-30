package com.anomaly.consumer;

final class RollingWindow {

    private final double[] values;
    private int size;
    private int next;

    RollingWindow(int capacity) {
        if (capacity < 2) {
            throw new IllegalArgumentException("window capacity must be at least 2, was " + capacity);
        }
        this.values = new double[capacity];
    }

    void add(double value) {
        if (!Double.isFinite(value)) {
            throw new IllegalArgumentException("window values must be finite, was " + value);
        }
        values[next] = value;
        next = (next + 1) % values.length;
        size = Math.min(size + 1, values.length);
    }

    int size() {
        return size;
    }

    int capacity() {
        return values.length;
    }

    boolean isFull() {
        return size == values.length;
    }

    double mean() {
        requireSamples(1);
        var total = 0.0;
        for (var i = 0; i < size; i++) {
            total += values[i];
        }
        return total / size;
    }

    double standardDeviation() {
        requireSamples(2);
        var mean = mean();
        var sumOfSquaredDeviations = 0.0;
        for (var i = 0; i < size; i++) {
            var deviation = values[i] - mean;
            sumOfSquaredDeviations += deviation * deviation;
        }
        return Math.sqrt(sumOfSquaredDeviations / (size - 1));
    }

    private void requireSamples(int required) {
        if (size < required) {
            throw new IllegalStateException(
                    "need at least " + required + " value(s) in the window, have " + size);
        }
    }
}
