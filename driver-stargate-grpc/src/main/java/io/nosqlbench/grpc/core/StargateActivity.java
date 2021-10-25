package io.nosqlbench.grpc.core;

import io.nosqlbench.activitytype.cql.statements.core.AvailableCQLStatements;
import io.nosqlbench.activitytype.cql.statements.core.CQLStatementDef;
import io.nosqlbench.activitytype.cql.statements.core.TaggedCQLStatementDefs;
import io.nosqlbench.activitytype.cql.statements.core.YamlCQLStatementLoader;
import io.nosqlbench.activitytype.cql.statements.rowoperators.verification.VerifierBuilder;
import io.nosqlbench.engine.api.activityapi.core.Activity;
import io.nosqlbench.engine.api.activityapi.core.ActivityDefObserver;
import io.nosqlbench.engine.api.activityapi.planning.OpSequence;
import io.nosqlbench.engine.api.activityapi.planning.SequencePlanner;
import io.nosqlbench.engine.api.activityapi.planning.SequencerType;
import io.nosqlbench.engine.api.activityconfig.ParsedStmt;
import io.nosqlbench.engine.api.activityconfig.StatementsLoader;
import io.nosqlbench.engine.api.activityconfig.rawyaml.RawStmtDef;
import io.nosqlbench.engine.api.activityconfig.rawyaml.RawStmtsBlock;
import io.nosqlbench.engine.api.activityconfig.rawyaml.RawStmtsDoc;
import io.nosqlbench.engine.api.activityconfig.rawyaml.RawStmtsDocList;
import io.nosqlbench.engine.api.activityconfig.yaml.OpTemplate;
import io.nosqlbench.engine.api.activityconfig.yaml.StmtsDocList;
import io.nosqlbench.engine.api.activityimpl.ActivityDef;
import io.nosqlbench.engine.api.activityimpl.ParameterMap;
import io.nosqlbench.engine.api.activityimpl.SimpleActivity;
import io.nosqlbench.engine.api.metrics.ExceptionCountMetrics;
import io.nosqlbench.engine.api.metrics.ExceptionHistoMetrics;
import io.nosqlbench.engine.api.templating.StrInterpolator;
import io.nosqlbench.engine.api.util.TagFilter;
import io.nosqlbench.grpc.binders.ValuesBinder;
import io.nosqlbench.grpc.core.StubCache.StargateBearerToken;
import io.nosqlbench.nb.api.errors.BasicError;
import io.nosqlbench.virtdata.core.bindings.Bindings;
import io.nosqlbench.virtdata.core.bindings.BindingsTemplate;
import io.nosqlbench.virtdata.core.bindings.ContextualArrayBindings;
import io.nosqlbench.virtdata.core.bindings.ContextualBindingsArrayTemplate;
import io.stargate.proto.QueryOuterClass.Consistency;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.StargateGrpc;
import io.stargate.proto.StargateGrpc.StargateBlockingStub;
import io.stargate.proto.StargateGrpc.StargateFutureStub;
import io.stargate.proto.StargateGrpc.StargateStub;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Optional;
import java.util.concurrent.Executor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SuppressWarnings("Duplicates")
public class StargateActivity extends SimpleActivity implements Activity, ActivityDefObserver {
    private final static Logger logger = LogManager.getLogger(
        StargateActivity.class);
    private OpSequence<Request> opsequence;

    private ConcurrentHashMap<StargateActionException, ExceptionMetaData> exceptionInfo = new ConcurrentHashMap<>();
    public static final long MILLIS_BETWEEN_SIMILAR_ERROR = 1000 * 60 * 10; // 10 minutes
    private final ExceptionCountMetrics exceptionCountMetrics;
    private final ExceptionHistoMetrics exceptionHistoMetrics;

    private int maxPages;
    private int maxTries;
    private long retryDelay;
    private long maxRetryDelay;
    private long requestDeadlineMs;
    private Integer numberOfConcurrentClients;

    private static final StubCache<StargateFutureStub> stubCache = new StubCache<>();


    public StargateActivity(ActivityDef activityDef) {
        super(activityDef);
        this.activityDef = activityDef;
        exceptionCountMetrics = new ExceptionCountMetrics(activityDef);
        exceptionHistoMetrics = new ExceptionHistoMetrics(activityDef);
    }

    public StargateFutureStub getStub() {
        return stubCache.get();
    }

    @Override
    public synchronized void initActivity() {
        logger.debug("initializing activity: " + this.activityDef.getAlias());

        initSequencer();
        setDefaultsFromOpSequence(this.opsequence);

        logger.debug("activity fully initialized: " + this.activityDef.getAlias());
    }

    public ConcurrentHashMap<StargateActionException, ExceptionMetaData> getExceptionInfo() {
        return exceptionInfo;
    }

    private void initSequencer() {
        SequencerType sequencerType = SequencerType.valueOf(
            getParams().getOptionalString("seq").orElse("bucket")
        );

        SequencePlanner<Request> planner = new SequencePlanner<>(sequencerType);

        StmtsDocList unfiltered = loadStmtsYaml();

        // log tag filtering results
        String tagfilter = activityDef.getParams().getOptionalString("tags").orElse("");
        TagFilter tagFilter = new TagFilter(tagfilter);
        unfiltered.getStmts().stream().map(tagFilter::matchesTaggedResult).forEach(r -> logger.debug(r.getLog()));

        List<OpTemplate> stmts = unfiltered.getStmts(tagfilter);

        if (stmts.size() == 0) {
            throw new RuntimeException("There were no unfiltered statements found for this activity.");
        }

        for (OpTemplate stmtDef : stmts) {
            ParsedStmt parsed = stmtDef.getParsed(s -> s).orError();

            ContextualBindingsArrayTemplate<Object, Payload> bindingTemplate = new ContextualBindingsArrayTemplate<>(null, new BindingsTemplate(), new ValuesBinder());

            bindingTemplate.getBindingsTemplate().addFieldBindings(parsed.getBindPoints());

            long ratio = stmtDef.getParamOrDefault("ratio", 1);

            ImmutableRequest.Builder builder = ImmutableRequest.builder()
                .name(parsed.getName())
                .parameterized(stmtDef.getParamOrDefault("parameterized", false))
                .idempotent(stmtDef.getOptionalStringParam("idempotent", Boolean.class).orElse(false))
                .cql(parsed.getPositionalStatement(s -> "?"))
                .bindings(bindingTemplate.resolveBindings())
                .consistency(stmtDef.getOptionalStringParam("cl", String.class).map(Consistency::valueOf))
                .serialConsistency(stmtDef.getOptionalStringParam("serial_cl").map(Consistency::valueOf))
                .ratio(ratio);

            if (activityDef.getParams().containsKey("verify") ||
                stmtDef.getParams().containsKey("verify") ||
                stmtDef.getParams().containsKey("verify-fields")) {
                builder.verifierBindings(VerifierBuilder.getExpectedValuesTemplate(stmtDef).resolveBindings());
            }

            planner.addOp(builder.build(), ratio);
        }

        opsequence = planner.resolve();
    }

    @Override
    public void shutdownActivity() {
        super.shutdownActivity();
    }

    @Override
    public String toString() {
        return "StargateActivity {" +
            "activityDef=" + activityDef +
            ", opSequence=" + this.opsequence +
            '}';
    }

    @Override
    public void onActivityDefUpdate(ActivityDef activityDef) {
        super.onActivityDefUpdate(activityDef);
        ParameterMap params = activityDef.getParams();
        this.maxPages = params.getOptionalInteger("maxpages").orElse(1);
        this.maxTries = params.getOptionalInteger("maxtries").orElse(10);
        this.retryDelay = params.getOptionalLong("retrydelay").orElse(0L);
        this.maxRetryDelay = params.getOptionalLong("maxretrydelay").orElse(500L);
        this.requestDeadlineMs = params.getOptionalLong("requestdeadline").orElse(5000L); // 5 seconds
        this.numberOfConcurrentClients = params.getOptionalInteger("number_of_concurrent_clients").orElse(1);

        initializeGrpcStubs(numberOfConcurrentClients);
    }

    private void initializeGrpcStubs(Integer numberOfConcurrentClients) {
        stubCache.build(activityDef, StargateGrpc::newFutureStub, numberOfConcurrentClients);
    }

    public int getMaxPages() {
        return maxPages;
    }

    public int getMaxTries() {
        return maxTries;
    }

    public long getRetryDelay() {
        return retryDelay;
    }

    public long getMaxRetryDelay() {
        return maxRetryDelay;
    }

    public long getRequestDeadlineMs() {
        return requestDeadlineMs;
    }

    public OpSequence<Request> getOpSequencer() {
        return opsequence;
    }

    public ExceptionCountMetrics getExceptionCountMetrics() {
        return exceptionCountMetrics;
    }

    public ExceptionHistoMetrics getExceptionHistoMetrics() {
        return exceptionHistoMetrics;
    }

    private StmtsDocList loadStmtsYaml() {
        StmtsDocList doclist = null;


        String yaml_loc = activityDef.getParams().getOptionalString("yaml", "workload").orElse("default");

        StrInterpolator interp = new StrInterpolator(activityDef);

        String yamlVersion = "unset";
        if (yaml_loc.endsWith(":1") || yaml_loc.endsWith(":2")) {
            yamlVersion = yaml_loc.substring(yaml_loc.length() - 1);
            yaml_loc = yaml_loc.substring(0, yaml_loc.length() - 2);
        }

        switch (yamlVersion) {
            case "1":
                doclist = getVersion1StmtsDoc(interp, yaml_loc);
                if (activityDef.getParams().getOptionalBoolean("ignore_important_warnings").orElse(false)) {
                    logger.warn("DEPRECATED-FORMAT: Loaded yaml " + yaml_loc + " with compatibility mode. " +
                        "This will be deprecated in a future release.");
                    logger.warn("DEPRECATED-FORMAT: Please refer to " +
                        "http://docs.engineblock.io/user-guide/standard_yaml/ for more details.");
                } else {
                    throw new BasicError("DEPRECATED-FORMAT: Loaded yaml " + yaml_loc + " with compatibility mode. " +
                        "This has been deprecated for a long time now. You should use the modern yaml format, which is easy" +
                        "to convert to. If you want to ignore this and kick the issue" +
                        " down the road to someone else, then you can add ignore_important_warnings=true. " +
                        "Please refer to " +
                        "http://docs.engineblock.io/user-guide/standard_yaml/ for more details.");
                }
                break;
            case "2":
                doclist = StatementsLoader.loadPath(logger, yaml_loc, interp, "activities");
                break;
            case "unset":
                try {
                    logger.debug("You can suffix your yaml filename or url with the " +
                        "format version, such as :1 or :2. Assuming version 2.");
                    doclist = StatementsLoader.loadPath(null, yaml_loc, interp, "activities");
                } catch (Exception ignored) {
                    try {
                        doclist = getVersion1StmtsDoc(interp, yaml_loc);
                        logger.warn("DEPRECATED-FORMAT: Loaded yaml " + yaml_loc +
                            " with compatibility mode. This will be deprecated in a future release.");
                        logger.warn("DEPRECATED-FORMAT: Please refer to " +
                            "http://docs.engineblock.io/user-guide/standard_yaml/ for more details.");
                    } catch (Exception compatError) {
                        logger.warn("Tried to load yaml in compatibility mode, " +
                            "since it failed to load with the standard format, " +
                            "but found an error:" + compatError);
                        logger.warn("The following detailed errors are provided only " +
                            "for the standard format. To force loading version 1 with detailed logging, add" +
                            " a version qualifier to your yaml filename or url like ':1'");
                        // retrigger the error again, this time with logging enabled.
                        doclist = StatementsLoader.loadPath(logger, yaml_loc, interp, "activities");
                    }
                }
                break;
            default:
                throw new RuntimeException("Unrecognized yaml format version, expected :1 or :2 " +
                    "at end of yaml file, but got " + yamlVersion + " instead.");
        }

        return doclist;

    }

    @Deprecated
    private StmtsDocList getVersion1StmtsDoc(StrInterpolator interp, String yaml_loc) {
        StmtsDocList unfiltered;
        List<RawStmtsBlock> blocks = new ArrayList<>();

        YamlCQLStatementLoader deprecatedLoader = new YamlCQLStatementLoader(interp);
        AvailableCQLStatements rawDocs = deprecatedLoader.load(yaml_loc, "activities");

        List<TaggedCQLStatementDefs> rawTagged = rawDocs.getRawTagged();

        for (TaggedCQLStatementDefs rawdef : rawTagged) {
            for (CQLStatementDef rawstmt : rawdef.getStatements()) {
                RawStmtsBlock rawblock = new RawStmtsBlock();

                // tags
                rawblock.setTags(rawdef.getTags());

                // params
                Map<String, Object> params = new HashMap<>(rawdef.getParams());
                if (rawstmt.getConsistencyLevel() != null && !rawstmt.getConsistencyLevel().isEmpty())
                    params.put("cl", rawstmt.getConsistencyLevel());
                if (!rawstmt.isPrepared()) params.put("prepared", "false");
                if (rawstmt.getRatio() != 1L)
                    params.put("ratio", String.valueOf(rawstmt.getRatio()));
                rawblock.setParams(params);


                // stmts
                List<RawStmtDef> stmtslist = new ArrayList<>();
                stmtslist.add(new RawStmtDef(rawstmt.getName(), rawstmt.getStatement()));
                rawblock.setRawStmtDefs(stmtslist);

                // bindings
                rawblock.setBindings(rawstmt.getBindings());

                blocks.add(rawblock);
            }
        }

        RawStmtsDoc rawStmtsDoc = new RawStmtsDoc();
        rawStmtsDoc.setBlocks(blocks);
        List<RawStmtsDoc> rawStmtsDocs = new ArrayList<>();
        rawStmtsDocs.add(rawStmtsDoc);
        RawStmtsDocList rawStmtsDocList = new RawStmtsDocList(rawStmtsDocs);
        unfiltered = new StmtsDocList(rawStmtsDocList);

        return unfiltered;
    }

}
