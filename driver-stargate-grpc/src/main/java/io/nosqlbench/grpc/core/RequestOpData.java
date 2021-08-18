package io.nosqlbench.grpc.core;

public class RequestOpData {
    final long cycle;
    final StargateAsyncAction action;

    public RequestOpData(long cycle, StargateAsyncAction action) {
        this.cycle = cycle;
        this.action = action;
    }
}
