package io.nosqlbench.grpc.core;

import com.google.common.collect.Maps;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.AbstractStub;
import io.nosqlbench.engine.api.activityapi.core.Shutdownable;
import io.nosqlbench.engine.api.activityimpl.ActivityDef;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StubCache<S extends AbstractStub<S>> implements Shutdownable {
    private final static Logger logger = LogManager.getLogger(StubCache.class);

    Map<String, S> entries = Maps.newConcurrentMap();

    public S get(ActivityDef def, Function<ManagedChannel, S> construct) {
        String key = def.getParams().getOptionalString("clusterid").orElse("default");
        return entries.computeIfAbsent(key, (k) -> build(def, construct));
    }

    private S build(ActivityDef def, Function<ManagedChannel, S> construct) {
        String host = def.getParams().getOptionalString("host").orElse("localhost");
        int port = def.getParams().getOptionalInteger("port").orElse(8090);

        // It is most convenient to call this `auth_token` because the NoSQLBench module running with Fallout already uses the name auth_token for other Stargate activities
        String token = def.getParams().getOptionalString("auth_token").orElseThrow(() -> new RuntimeException("No auth token configured for gRPC driver"));

        ManagedChannel channel =
            ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext() // TODO support SSL
                .build();

        return construct.apply(channel).withCallCredentials(new StargateBearerToken(token));
    }



    @Override
    public void shutdown() {
        for (S stub : entries.values()) {
            try {
                ((ManagedChannel)stub.getChannel()).shutdown().awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("Failed to shutdown stub's channel", e);
            }
        }
    }

    public static class StargateBearerToken extends CallCredentials {
        public static final Metadata.Key<String> TOKEN_KEY =
            Metadata.Key.of("X-Cassandra-Token", Metadata.ASCII_STRING_MARSHALLER);

        private final String token;

        public StargateBearerToken(String token) {
            this.token = token;
        }

        @Override
        public void applyRequestMetadata(
            RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
            appExecutor.execute(
                () -> {
                    try {
                        Metadata metadata = new Metadata();
                        metadata.put(TOKEN_KEY, token);
                        applier.apply(metadata);
                    } catch (Exception e) {
                        applier.fail(Status.UNAUTHENTICATED.withCause(e));
                    }
                });
        }

        @Override
        public void thisUsesUnstableApi() {}
    }
}
