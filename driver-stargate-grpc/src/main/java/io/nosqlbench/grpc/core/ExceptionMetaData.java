package io.nosqlbench.grpc.core;

public class ExceptionMetaData {
    long timeWritten;
    int numSquelchedInstances;
    public ExceptionMetaData() {
        this.timeWritten = System.currentTimeMillis();
        this.numSquelchedInstances = 0;
    }

    public long timeWritten() {
        return this.timeWritten;
    }

    public long numSquelchedInstances() {
        return this.numSquelchedInstances;
    }

    public ExceptionMetaData increment() {
        numSquelchedInstances++;
        return this;
    }
}