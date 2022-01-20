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
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import io.stargate.proto.ReactorStargateGrpc.ReactorStargateStub;
import io.stargate.proto.StargateGrpc.StargateFutureStub;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StubCache implements Shutdownable {
    private final static Logger logger = LogManager.getLogger(StubCache.class);

    Map<Integer, StargateFutureStub> futureStubs = Maps.newConcurrentMap();
    Map<Integer, ReactorStargateStub> reactorStubs = Maps.newConcurrentMap();
    AtomicInteger counter = new AtomicInteger();

    /**
     * @return the AbstractStub in a round-robin fashion
     */
    public StargateFutureStub get() {
        int numberOfEntries = futureStubs.size();
        int index = (counter.getAndUpdate(value -> (value + 1) % numberOfEntries));
        return futureStubs.get(index);
    }


    /**
     * @return the ReactorStargateStub in a round-robin fashion
     */
    public ReactorStargateStub getReactorStub() {
        int numberOfEntries = reactorStubs.size();
        int index = (counter.getAndUpdate(value -> (value + 1) % numberOfEntries));
        return reactorStubs.get(index);
    }

    /**
     * Initializes N number of StargateGrpc clients, each with a dedicated channel.
     * @param numberOfConcurrentClients number of clients to initialize.
     */
    public void build(ActivityDef def,
                      Function<ManagedChannel, StargateFutureStub> constructFutureStub,
                      Function<ManagedChannel, ReactorStargateStub> constructReactiveStub,
                      int numberOfConcurrentClients) {
        for(int i = 0; i < numberOfConcurrentClients; i++){
            futureStubs.computeIfAbsent(i, (k) -> build(def, constructFutureStub));
            reactorStubs.computeIfAbsent(i, (k) -> build(def, constructReactiveStub));
        }
    }

    private <S extends AbstractStub<S>> S build(ActivityDef def,
                                                              Function<ManagedChannel, S> construct) {
        Optional<String> hostsOpt = def.getParams().getOptionalString("hosts");
        Optional<String> hostOpt = def.getParams().getOptionalString("host");
        String host = hostsOpt.orElseGet(() -> hostOpt.orElseThrow(() -> new RuntimeException("`hosts` or `host` are required")));

        int port = def.getParams().getOptionalInteger("port").orElse(8090);
        // plainText should when running a Stargate directly. When connecting to astra, it should be set to false.
        boolean usePlaintext = def.getParams().getOptionalBoolean("use_plaintext").orElse(true);

        // It is most convenient to call this `auth_token` because the NoSQLBench module running with Fallout already uses the name auth_token for other Stargate activities
        String token = def.getParams().getOptionalString("auth_token").orElseThrow(() -> new RuntimeException("No auth token configured for gRPC driver"));

        logger.info("Building channel for host: {} port: {} token: {} usePlaintext: {} " , host, port, token, usePlaintext);
        ManagedChannel channel;
        if(usePlaintext) {
            channel =
                ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .directExecutor()
                    .build();
        }else{
            channel =
                ManagedChannelBuilder.forAddress(host, port)
                    .directExecutor()
                    .build();
        }

        return construct.apply(channel).withCallCredentials(new StargateBearerToken(token));
    }



    @Override
    public void shutdown() {
        for (StargateFutureStub stub : futureStubs.values()) {
            close(stub);
        }
        for (ReactorStargateStub stub : reactorStubs.values()) {
            close(stub);
        }
    }

    private<T extends AbstractStub<T>> void close(T stub) {
        try {
            ((ManagedChannel) stub.getChannel()).shutdown().awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Failed to shutdown stub's channel", e);
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
