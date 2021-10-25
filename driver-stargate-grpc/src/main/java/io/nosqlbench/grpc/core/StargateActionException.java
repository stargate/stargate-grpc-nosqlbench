package io.nosqlbench.grpc.core;

public class StargateActionException extends Throwable {
    private Throwable wrapped;
    public StargateActionException(Throwable e) {
        this.wrapped = e;
    }

    @Override
    public int hashCode() {
        return wrapped.getMessage().hashCode();
    }

    @Override
    public String getMessage() {
        return wrapped.getMessage();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (!(other instanceof StargateActionException)) return false;
        StargateActionException otherException = (StargateActionException) other;
        return otherException.getMessage().equals(wrapped.getMessage());
    }
}