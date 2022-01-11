package io.nosqlbench.grpc.core;

import java.util.Objects;

public class ReactiveStargateActionException extends Throwable {
    private final String msg;
    private final int code;

    public ReactiveStargateActionException(String msg, int code) {
        super(msg);
        this.msg = msg;
        this.code = code;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReactiveStargateActionException that = (ReactiveStargateActionException) o;
        return code == that.code && Objects.equals(msg, that.msg);
    }

    @Override
    public int hashCode() {
        return Objects.hash(msg, code);
    }

    @Override
    public String toString() {
        return "ReactiveStargateActionException{" +
            "msg='" + msg + '\'' +
            ", code=" + code +
            '}';
    }
}
