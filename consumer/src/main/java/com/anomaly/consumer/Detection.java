package com.anomaly.consumer;

record Detection(double value, double mean, double standardDeviation, double zScore, Status status) {

    enum Status {
        WARMING_UP,
        NORMAL,
        ANOMALY
    }

    boolean isAnomaly() {
        return status == Status.ANOMALY;
    }
}
