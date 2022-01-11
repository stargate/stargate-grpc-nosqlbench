package io.nosqlbench.grpc.core;

import io.stargate.proto.QueryOuterClass;
import reactor.core.publisher.FluxSink;

public class NewQueryListener {

    private final FluxSink<QueryOuterClass.Query> fluxSink;

    public NewQueryListener(FluxSink<QueryOuterClass.Query> fluxSink) {
        this.fluxSink = fluxSink;
    }

    void onQuery(QueryOuterClass.Query query){
        fluxSink.next(query);
    }

    void complete(){
//        sink.complete();
    }

}
