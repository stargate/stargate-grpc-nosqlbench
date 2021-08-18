package io.nosqlbench.grpc.core;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import io.nosqlbench.activitytype.cql.errorhandling.HashedCQLErrorHandler;
import io.nosqlbench.engine.api.activityapi.core.BaseAsyncAction;
import io.nosqlbench.engine.api.activityapi.core.ops.fluent.opfacets.StartedOp;
import io.nosqlbench.engine.api.activityapi.core.ops.fluent.opfacets.TrackedOp;
import io.nosqlbench.engine.api.activityapi.planning.OpSequence;
import io.nosqlbench.engine.api.activityimpl.ActivityDef;
import java.util.function.LongFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// TODO: Implement me
@SuppressWarnings("Duplicates")
public class StargateAsyncAction extends BaseAsyncAction<RequestOpData, StargateActivity> {

    private final static Logger logger = LogManager.getLogger(
        StargateAsyncAction.class);
    private final ActivityDef activityDef;

    private OpSequence<Request> sequencer;
    private Timer bindTimer;
    private Timer executeTimer;
    private Timer resultTimer;
    private Timer resultSuccessTimer;
    private Histogram triesHisto;

    public StargateAsyncAction(StargateActivity activity, int slot) {
        super(activity, slot);
        onActivityDefUpdate(activity.getActivityDef());
        this.activityDef = activity.getActivityDef();
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
    public LongFunction<RequestOpData> getOpInitFunction() {
        return (l) -> {
            return new RequestOpData(l, this);
        };
    }

    @Override
    public void startOpCycle(TrackedOp<RequestOpData> opc) {
        throw new UnsupportedOperationException("Not implemented");
    }


    public void onSuccess(StartedOp<RequestOpData> sop) {
    }

    public void onFailure(StartedOp<RequestOpData> startedOp) {

    }


    @Override
    public void onActivityDefUpdate(ActivityDef activityDef) {
    }

    public String toString() {
        return "StargateAsyncAction["+this.slot+"]";
    }
}
