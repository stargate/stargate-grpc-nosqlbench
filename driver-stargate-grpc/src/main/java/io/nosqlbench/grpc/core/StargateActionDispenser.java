package io.nosqlbench.grpc.core;


import io.nosqlbench.engine.api.activityapi.core.Action;
import io.nosqlbench.engine.api.activityapi.core.ActionDispenser;

public class StargateActionDispenser implements ActionDispenser {

    public StargateActivity getActivity() {
        return activity;
    }

    private StargateActivity activity;

    public StargateActionDispenser(StargateActivity activityContext) {
        this.activity = activityContext;
    }

    public Action getAction(int slot) {
        long async= activity.getActivityDef().getParams().getOptionalLong("async").orElse(0L);
        if (async>0) {
            return new StargateAsyncAction(activity, slot);
        } else {
            return new StargateAction(activity.getActivityDef(), slot, activity);
        }
    }
}
