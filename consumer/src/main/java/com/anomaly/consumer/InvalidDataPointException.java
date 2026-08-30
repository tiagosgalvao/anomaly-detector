package com.anomaly.consumer;

class InvalidDataPointException extends RuntimeException {

    InvalidDataPointException(String message) {
        super(message);
    }
}
