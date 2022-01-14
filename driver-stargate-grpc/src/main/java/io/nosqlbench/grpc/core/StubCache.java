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
import java.util.stream.Collectors;

import io.stargate.proto.QueryOuterClass;
import io.stargate.proto.ReactorStargateGrpc.ReactorStargateStub;
import io.stargate.proto.StargateGrpc.StargateFutureStub;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

public class StubCache implements Shutdownable {
    private final static Logger logger = LogManager.getLogger(StubCache.class);

    Map<Integer, StargateFutureStub> futureStubs = Maps.newConcurrentMap();
    Map<Integer, ReactiveState> reactiveState = Maps.newConcurrentMap();
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
     *
     * @return the ReactiveState associated in a round-robin fashion
     */
    public ReactiveState getReactiveState() {
        int numberOfEntries = reactiveState.size();
        int index = (counter.getAndUpdate(value -> (value + 1) % numberOfEntries));
        return reactiveState.get(index);
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
            reactiveState.computeIfAbsent(i, (k) -> new ReactiveState(build(def,
                constructReactiveStub)));
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
        for (ReactorStargateStub stub : reactiveState.values().stream().map(v->v.reactorStargateStub).collect(Collectors.toList())) {
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

    public static class ReactiveState {
        Flux<QueryOuterClass.Query> queryFlux;
        NewQueryListener listener;
        final ReactorStargateStub reactorStargateStub;
        private Flux<QueryOuterClass.StreamingResponse> responseFlux;
        private Disposable subscription;

        public ReactiveState(ReactorStargateStub reactorStargateStub) {
            this.reactorStargateStub = reactorStargateStub;
        }

        public boolean fluxCreated(){
            return queryFlux != null;
        }

        public void registerListener(NewQueryListener newQueryListener){
            System.out.println("register new listener:" + newQueryListener);
            listener = newQueryListener;
        }

        public void setQueryFlux(Flux<QueryOuterClass.Query> flux) {
            this.queryFlux = flux;
        }

        public NewQueryListener getListener() {
            return listener;
        }

        public void onQuery(QueryOuterClass.Query q)  {
            System.out.println("listener on query");
            listener.onQuery(q);
        }

        public void setResponseFlux(Flux<QueryOuterClass.StreamingResponse> responseFlux) {
            this.responseFlux = responseFlux;

        }

        public Flux<QueryOuterClass.StreamingResponse> getResponseFlux() {
            return responseFlux;
        }

        public void setSubscription(Disposable subscription) {
            this.subscription = subscription;
        }

        public boolean isSubscriptionCreated() {
            return subscription!=null;
        }
    }
}
