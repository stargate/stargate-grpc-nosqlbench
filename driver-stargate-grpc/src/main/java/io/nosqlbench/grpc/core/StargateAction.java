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
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    }

    @Override
    public int runCycle(long cycle) {
        return runPhase(cycle);
    }

    @Override
    public int runPhase(long cycle) {
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
                if (!shouldRetry(e)) {
                    handleErrorLogging(e);
                    triesHisto.update(tries);
                    pagingState = null;
                    return -1;
                }
            } finally {
                if (resultTime != null) {
                    resultTime.stop();
                }
            }
        }

        triesHisto.update(tries);
        return 0;
    }

    private void handleErrorLogging(Exception e) {
        StargateActionException sae = new StargateActionException(e);
        long now = System.currentTimeMillis();
        if (this.activity.getExceptionInfo().containsKey(sae)) {
            ExceptionMetaData metadata = this.activity.getExceptionInfo().get(sae);
            if (now - metadata.timeWritten() > StargateActivity.MILLIS_BETWEEN_SIMILAR_ERROR) {
                String numberOfMessages = String.format("[%d times]", metadata.numSquelchedInstances());
                logger.error("Unable to retry request with errors " + numberOfMessages, e);
                this.activity.getExceptionInfo().compute(sae, (key, value) -> new ExceptionMetaData());
            } else {
                this.activity.getExceptionInfo().compute(sae, (key, value) -> value.increment());
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

    private boolean shouldRetry(Exception e) {
        Status status = null;
        if (e instanceof StatusException) {
            status = ((StatusException) e).getStatus();
        } else if (e instanceof StatusRuntimeException) {
            status = ((StatusRuntimeException) e).getStatus();
        } else if (e instanceof ExecutionException) {
            Throwable cause = e.getCause();
            if (cause != null && cause instanceof StatusException) {
                status = ((StatusException) cause).getStatus();
            } else if (cause != null && cause instanceof StatusRuntimeException) {
                status = ((StatusRuntimeException) cause).getStatus();
            }
        }

        // Retry if there's an unavailable exception or read/write timeout
        return status != null && (status.getCode() == Status.UNAVAILABLE.getCode()
            || status.getCode() == Status.DEADLINE_EXCEEDED.getCode());
    }
}
