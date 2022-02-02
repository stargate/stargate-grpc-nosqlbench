package io.nosqlbench.grpc.core;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import io.grpc.Deadline;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.nosqlbench.engine.api.activityapi.core.ActivityDefObserver;
import io.nosqlbench.engine.api.activityapi.core.MultiPhaseAction;
import io.nosqlbench.engine.api.activityapi.core.SyncAction;
import io.nosqlbench.engine.api.activityapi.planning.OpSequence;
import io.nosqlbench.engine.api.activityimpl.ActivityDef;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.ConsistencyValue;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.Response;
import io.stargate.proto.QueryOuterClass.ResultSet;
import io.stargate.proto.QueryOuterClass.Values;
import io.stargate.proto.StargateGrpc.StargateFutureStub;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.Disposable;

@SuppressWarnings("Duplicates")
public class StargateAction implements SyncAction, MultiPhaseAction, ActivityDefObserver {
    private final static Logger logger = LogManager.getLogger(
        StargateAction.class);

    private final int slot;
    private final StargateActivity activity;
    private final ActivityDef activityDef;
    private OpSequence<Request> sequencer;
    private ByteString pagingState;

    private Timer bindTimer;
    private Timer executeTimer;
    private Timer resultTimer;
    private Timer resultSuccessTimer;
    private Histogram triesHisto;

    private long startTime;
    private int maxPages;
    private int maxTries;
    private long retryDelay;
    private long maxRetryDelay;
    private long requestDeadlineMs;

    private int numPages;

    private boolean biStreaming;

    public StargateAction(ActivityDef activityDef, int slot, StargateActivity cqlActivity) {
        this.activityDef = activityDef;
        this.activity = cqlActivity;
        this.slot = slot;
        onActivityDefUpdate(activityDef);
    }

    @Override
    public void init() {
        onActivityDefUpdate(activityDef);
        this.sequencer = activity.getOpSequencer();
        this.bindTimer = activity.getInstrumentation().getOrCreateBindTimer();
        this.executeTimer = activity.getInstrumentation().getOrCreateExecuteTimer();
        this.resultTimer = activity.getInstrumentation().getOrCreateResultTimer();
        this.resultSuccessTimer = activity.getInstrumentation().getOrCreateResultSuccessTimer();
        this.triesHisto = activity.getInstrumentation().getOrCreateTriesHistogram();
        this.biStreaming = activity.getParams()
            .getOptionalBoolean("bi_streaming").orElse(false);
    }

    @Override
    public int runCycle(long cycle) {
        return runPhase(cycle);
    }

    @Override
    public int runPhase(long cycle) {
        if (biStreaming) {
            return runStreaming(cycle);
        } else {
            return runStandard(cycle);
        }
    }

    private int runStreaming(long cycle) {
        Request request = sequencer.get(cycle);

       // startTimeRef.set(System.nanoTime());
       // it was moved into executeQueryReactive()

        StargateActivity.ReactiveState reactiveState;

        QueryParameters.Builder paramsBuilder = QueryParameters.newBuilder();
        request.consistency().ifPresent(cl -> paramsBuilder.setConsistency(ConsistencyValue.newBuilder().setValue(cl)));
        request.serialConsistency().ifPresent(cl -> paramsBuilder.setSerialConsistency(ConsistencyValue.newBuilder().setValue(cl)));
        Query.Builder queryBuilder = Query.newBuilder().setCql(request.cql());

        try (Timer.Context ignored = bindTimer.time()) {
            Values values = request.bindings().bind(cycle);
            if (values != null) {
                queryBuilder.setValues(values);
            }
        }

        try (Timer.Context ignored = executeTimer.time()) {
            Query query = queryBuilder.build();
            // execute query, it will be propagated via listener API to the Query flux.
            reactiveState = activity.executeQueryReactive(query);
        }

        reactiveState.setResultTimer(resultTimer.time());

        if (!reactiveState.isSubscriptionCreated()) {
            // build execution flow
            logger.debug("Constructing new flow");
            Disposable subscription = reactiveState.getResponseFlux().doOnNext(
                r -> {
                    Response response = r.getResponse();
                    logger.debug("doOnNext, received: " + response);
                    if (response.hasResultSet()) {
                        ResultSet rs = response.getResultSet();
                        request.verifierBindings().ifPresent(bindings -> {
                            // Only checking field names for now which matches the functionality of the driver-cql-shaded
                            Map<String, Object> expectedValues = bindings.getLazyMap(cycle);
                            Set<String> fields = rs.getColumnsList().stream()
                                .map(ColumnSpec::getName).collect(Collectors.toSet());
                            List<String> missingFields = expectedValues.keySet().stream()
                                .filter(k -> !fields.contains(k)).collect(Collectors.toList());
                            if (!missingFields.isEmpty()) {
                                throw new RuntimeException(
                                    String.format("Missing columns %s on cycle %d",
                                        String.join(", ", missingFields), cycle));
                            }
                        });

                        resultSuccessTimer
                            .update(System.nanoTime() - reactiveState.getStartTime(), TimeUnit.NANOSECONDS);
                        reactiveState.complete(0);

                    } else {
                        com.google.rpc.Status status = r.getStatus();
                        if (status.getCode() == 0) {
                            // it is a success response without any result set
                            resultSuccessTimer
                                .update(System.nanoTime() - reactiveState.getStartTime(), TimeUnit.NANOSECONDS);
                            reactiveState.complete(0);
                        } else {
                            logger.debug("Error, message: " + status.getMessage());
                            // it is an error
                            long resultNanos = reactiveState.stopResultSetTimer();
                            reactiveState.clearResultSetTimer();
                            activity.getExceptionCountMetrics().count(String.valueOf(status.getCode()));
                            activity.getExceptionHistoMetrics().update(String.valueOf(status.getCode()), resultNanos);
                            handleErrorLogging(status);
                            triesHisto.update(1);
                            reactiveState.complete(-1);
                        }
                    }

                }
            ).onErrorContinue((e,v) -> {
                logger.warn("onError, for value: " + v + " it will continue the processing.", e);
                long resultNanos = reactiveState.stopResultSetTimer();
                reactiveState.clearResultSetTimer();
                activity.getExceptionCountMetrics().count(e.getClass().getSimpleName());
                activity.getExceptionHistoMetrics().update(e.getClass().getSimpleName(), resultNanos);
                handleErrorLogging(e);
                // update every time we get retry
                triesHisto.update(1);
                reactiveState.complete(-1);
            }).doOnEach(e -> {
                if(reactiveState.isDone()){
                    reactiveState.complete(1);
                }
                reactiveState.stopResultSetTimer();
            }).subscribe();
            // The whole reactive workflow is assembled and the new subscription is created only for the first invocation per thread.
            // Once it is created, this workflow is reused and the queries are propagated via listener API.
            reactiveState.setSubscription(subscription);
        }

        try {
            logger.debug("waiting for completion: {} from Thread: {}", reactiveState.getCompletion(),Thread.currentThread().getName());
            Integer result = reactiveState.getCompletion().get(5, TimeUnit.SECONDS);
            triesHisto.update(1);
            return result;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            logger.error("problem while waiting for completion for: " + reactiveState.getCompletion(), e);
            return -1;
        }
    }

    private int runStandard(long cycle) {
        Request request = sequencer.get(cycle);

        StargateFutureStub stub = activity
            .getStub()
            .withDeadline(Deadline
                .after(requestDeadlineMs, TimeUnit.MILLISECONDS)); // TODO: Is this expensive?

        if (pagingState == null) {
            numPages = 0;
            startTime = System.nanoTime();
        }

        boolean completed = false;
        int tries = 0;
        while (tries < maxTries && !completed) {
            tries++;

            if (tries >= maxTries) {
                handleErrorLogging(new RuntimeException("Exhausted max retries"));
            }

            if (tries > 1) {
                try {
                    Thread.sleep(Math.min((retryDelay << tries) / 1000, maxRetryDelay / 1000));
                } catch (InterruptedException ignored) {
                    // Do nothing
                }
            }

            ListenableFuture<Response> responseFuture;

            QueryParameters.Builder paramsBuilder = QueryParameters.newBuilder();
            if (pagingState != null) {
                paramsBuilder.setPagingState(BytesValue.of(pagingState));
            }

            request.consistency().ifPresent(cl -> paramsBuilder.setConsistency(ConsistencyValue.newBuilder().setValue(cl)));

            request.serialConsistency().ifPresent(cl -> paramsBuilder.setSerialConsistency(ConsistencyValue.newBuilder().setValue(cl)));

            Query.Builder queryBuilder = Query.newBuilder().setCql(request.cql());

            try (Timer.Context ignored = bindTimer.time()) {
                Values values = request.bindings().bind(cycle);
                if (values != null) {
                    queryBuilder.setValues(values);
                }
            }

            try (Timer.Context ignored = executeTimer.time()) {
                responseFuture = stub
                    .executeQuery(queryBuilder.build());
            }

            Timer.Context resultTime = resultTimer.time();
            try {
                Response response = responseFuture.get();

                if (response.hasResultSet()) {
                    ResultSet rs = response.getResultSet();

                    request.verifierBindings().ifPresent(bindings -> {
                        // Only checking field names for now which matches the functionality of the driver-cql-shaded
                        Map<String, Object> expectedValues = bindings.getLazyMap(cycle);
                        Set<String> fields = rs.getColumnsList().stream()
                            .map(ColumnSpec::getName).collect(Collectors.toSet());
                        List<String> missingFields = expectedValues.keySet().stream()
                            .filter(k -> !fields.contains(k)).collect(Collectors.toList());
                        if (!missingFields.isEmpty()) {
                            throw new RuntimeException(
                                String.format("Missing columns %s on cycle %d",
                                    String.join(", ", missingFields), cycle));
                        }
                    });

                    if (rs.hasPagingState()) {
                        pagingState = rs.getPagingState().getValue();
                        if (++numPages > maxPages) {
                            throw new RuntimeException("Exceeded the max number of pages");
                        }
                    } else {
                        resultSuccessTimer
                            .update(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
                        pagingState = null;
                    }
                } else {
                    resultSuccessTimer
                        .update(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
                    pagingState = null;
                }

                completed = true;
            } catch (Exception e) {
                long resultNanos = resultTime.stop();
                resultTime = null;
                activity.getExceptionCountMetrics().count(e.getClass().getSimpleName());
                activity.getExceptionHistoMetrics().update(e.getClass().getSimpleName(), resultNanos);
                handleErrorLogging(e);
                if (!shouldRetry(e)) {
                    pagingState = null;
                    return -1;
                }
                // update every time we get retry
                triesHisto.update(1);

            } finally {
                if (resultTime != null) {
                    resultTime.stop();
                }
            }
        }

        triesHisto.update(1);
        return 0;
    }


    private void handleErrorLogging(com.google.rpc.Status status) {
        ReactiveStargateActionException sae = new ReactiveStargateActionException(status.getMessage(), status.getCode());
        long now = System.currentTimeMillis();
        if (this.activity.getReactiveExceptionInfo().containsKey(sae)) {
            ExceptionMetaData metadata = this.activity.getReactiveExceptionInfo().get(sae);
            if (now - metadata.timeWritten() > StargateActivity.MILLIS_BETWEEN_SIMILAR_ERROR) {
                String numberOfMessages = String.format("[%d times]", metadata.numSquelchedInstances());
                logger.error("Unable to retry request with errors " + numberOfMessages + " " + sae);
                this.activity.getReactiveExceptionInfo().compute(sae, (key, value) -> new ExceptionMetaData());
            } else {
                this.activity.getReactiveExceptionInfo().computeIfPresent(sae, (key, value) -> value.increment());
            }
        } else {
            logger.error("Unable to retry request with error [first encounter]" + " " + sae);
            this.activity.getReactiveExceptionInfo().put(sae, new ExceptionMetaData());
        }
    }


    private void handleErrorLogging(Throwable e) {
        StargateActionException sae = new StargateActionException(e);
        long now = System.currentTimeMillis();
        if (this.activity.getExceptionInfo().containsKey(sae)) {
            ExceptionMetaData metadata = this.activity.getExceptionInfo().get(sae);
            if (now - metadata.timeWritten() > StargateActivity.MILLIS_BETWEEN_SIMILAR_ERROR) {
                String numberOfMessages = String.format("[%d times]", metadata.numSquelchedInstances());
                logger.error("Unable to retry request with errors " + numberOfMessages, e);
                this.activity.getExceptionInfo().compute(sae, (key, value) -> new ExceptionMetaData());
            } else {
                this.activity.getExceptionInfo().computeIfPresent(sae, (key, value) -> value.increment());
            }
        } else {
            logger.error("Unable to retry request with error [first encounter]", e);
            this.activity.getExceptionInfo().put(sae, new ExceptionMetaData());
        }
    }

    @Override
    public boolean incomplete() {
        return pagingState != null;
    }

    @Override
    public void onActivityDefUpdate(ActivityDef activityDef) {
        this.maxPages = activity.getMaxPages();
        this.maxTries = activity.getMaxTries();
        this.retryDelay = activity.getRetryDelay();
        this.maxRetryDelay = activity.getMaxRetryDelay();
        this.requestDeadlineMs = activity.getRequestDeadlineMs();
    }

    @Override
    public String toString() {
        return "StargateAction[" + this.slot + "]";
    }


    private boolean shouldRetry(com.google.rpc.Status status) {
        // Retry if there's an unavailable exception or read/write timeout
        return status != null && (status.getCode() == Status.UNAVAILABLE.getCode().value()
            || status.getCode() == Status.DEADLINE_EXCEEDED.getCode().value());
    }


    private boolean shouldRetry(Throwable e) {
        Status status = null;
        if (e instanceof StatusException) {
            status = ((StatusException) e).getStatus();
        } else if (e instanceof StatusRuntimeException) {
            status = ((StatusRuntimeException) e).getStatus();
        } else if (e instanceof ExecutionException) {
            Throwable cause = e.getCause();
            if (cause instanceof StatusException) {
                status = ((StatusException) cause).getStatus();
            } else if (cause instanceof StatusRuntimeException) {
                status = ((StatusRuntimeException) cause).getStatus();
            }
        }

        // Retry if there's an unavailable exception or read/write timeout
        return status != null && (status.getCode() == Status.UNAVAILABLE.getCode()
            || status.getCode() == Status.DEADLINE_EXCEEDED.getCode());
    }
}
