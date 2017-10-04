package kic.pipeline;

import kic.dataframe.DataFrame;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import kic.dataframe.join.Join;
import kic.dataframe.join.JoinOperation;
import kic.dataframe.join.OuterJoinFillLastRow;
import kic.utils.MapUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * somehow it would be nice to have some kind of builder like
 * PiplineExecutor.getPrices()
 *                .toReturns()
 *                .objective()
 *                .optimize()
 *                .joinOLD(returns, objective)
 */
public class PipelineExecutor {
    private final Logger logger = LoggerFactory.getLogger(PipelineExecutor.class);
    private final Map<String, DataFrame> pipelineResults = new LinkedHashMap<>();
    private DataFrame lastResult;

    public static PipelineExecutor startFrom(String start, DataFrame initialDataframe) {
        return new PipelineExecutor(start, initialDataframe);
    }

    public PipelineExecutor(String startWith, DataFrame initialDataframe) {
        pipelineResults.put(startWith, initialDataframe);
        this.lastResult = initialDataframe;
    }

    public PipelineExecutor thenCalculate(String calculate, String using, Function<DataFrame, DataFrame> step) {
        logger.info("execute {} using {}", calculate, using);
        DataFrame stepResult = step.apply(pipelineResults.get(using));
        pipelineResults.put(calculate, stepResult);
        lastResult = stepResult;
        return this;
    }

    public PipelineExecutor thenCalculate(String calculate, Function<DataFrame, DataFrame> step) {
        logger.info("execute {}", calculate);
        DataFrame stepResult = step.apply(lastResult);
        pipelineResults.put(calculate, stepResult);
        lastResult = stepResult;
        return this;
    }

    public PipelineExecutor and(String calculate, Function<DataFrame, DataFrame> step) {
        logger.info("and execute {}", calculate);
        DataFrame[] dataFrames = pipelineResults.values().toArray(new DataFrame[0]);
        DataFrame stepResult = step.apply(dataFrames[dataFrames.length-2]);
        pipelineResults.put(calculate, stepResult);
        return this;
    }

    public PipelineExecutor passOverTo(Consumer<DataFrame> to) {
        logger.info("passOverTo last result to consumer");
        to.accept(lastResult);
        return this;
    }

    public PipelineExecutor passOverTo(String dataframe, Consumer<DataFrame> to) {
        logger.info("passOverTo {} to consumer", dataframe);
        to.accept(pipelineResults.get(dataframe));
        return this;
    }

    public PipelineExecutor join(String as, String using, Function<DataFrame, DataFrame> extractor, Map<String, JoinOperation> joins) {
        logger.info("join {}", joins.keySet());
        Join join = new Join(pipelineResults.get(using), using, extractor);
        joins.forEach((name, operation) -> join.left(pipelineResults.get(name), name, operation));
        pipelineResults.put(as, join.getJoinedDataFrame());
        lastResult = join.getJoinedDataFrame();
        return this;
    }

    public PipelineExecutor join(String as, Function<DataFrame, DataFrame> extractor, Map<String, JoinOperation> joins) {
        Map.Entry<String, DataFrame> lastEntry = pipelineResults.entrySet().toArray(new Map.Entry[0])[pipelineResults.size() - 1];
        logger.info("join {}", joins.keySet());
        Join join = new Join(lastEntry.getValue(), lastEntry.getKey(), extractor);
        joins.forEach((name, operation) -> join.left(pipelineResults.get(name), name, operation));
        pipelineResults.put(as, join.getJoinedDataFrame());
        lastResult = join.getJoinedDataFrame();
        return this;
    }

    public Map<String, DataFrame> getPipelineResults() {
        return pipelineResults;
    }
}
