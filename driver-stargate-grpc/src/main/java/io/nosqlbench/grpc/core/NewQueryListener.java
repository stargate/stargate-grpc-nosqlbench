package io.nosqlbench.grpc.core;

import io.stargate.proto.QueryOuterClass;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import reactor.core.publisher.FluxSink;

public class NewQueryListener {
    private final static Logger logger = LogManager.getLogger(NewQueryListener.class);

    private final FluxSink<QueryOuterClass.Query> fluxSink;

    public NewQueryListener(FluxSink<QueryOuterClass.Query> fluxSink) {
        this.fluxSink = fluxSink;
    }

    void onQuery(QueryOuterClass.Query query){
        logger.info("NewQueryListener, for fluxSink: " + fluxSink);
        fluxSink.next(query);
    }

    void complete(){
//        sink.complete();
    }

}
