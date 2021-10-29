package io.nosqlbench.grpc.core;

public class StargateActionException extends Throwable {
    public StargateActionException(Throwable t) {
        super(t);
    }

    @Override
    public int hashCode() {
        return this.getCause().getMessage().hashCode();
    }

    @Override
    public String getMessage() {
        return this.getCause().getMessage();
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) return false;
        if (!(other instanceof StargateActionException)) return false;
        StargateActionException otherException = (StargateActionException) other;
        return otherException.getCause().getMessage().equals(this.getCause().getMessage());
    }
}