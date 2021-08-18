package io.nosqlbench.grpc.core;

import io.nosqlbench.virtdata.core.bindings.Bindings;
import io.nosqlbench.virtdata.core.bindings.ContextualArrayBindings;
import io.stargate.proto.QueryOuterClass.Consistency;
import io.stargate.proto.QueryOuterClass.Payload;
import java.util.Optional;
import org.immutables.value.Value.Immutable;

@Immutable
interface Request {
    long ratio();

    String name();

    boolean parameterized();

    boolean idempotent();

    String cql();

    ContextualArrayBindings<Object, Payload> bindings();

    Optional<Bindings> verifierBindings();

    Optional<Consistency> consistency();

    Optional<Consistency> serialConsistency();
}
