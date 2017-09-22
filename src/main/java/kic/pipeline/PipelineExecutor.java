package kic.pipeline;

import kic.dataframe.DataFrame;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import kic.utils.MapUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * somehow it would be nice to have some kind of builder like
 * PiplineExecutor.getPrices()
 *                .toReturns()
 *                .objective()
 *                .optimize()
 *                .join(returns, objective)
 */
public class PipelineExecutor {
    private final Logger logger = LoggerFactory.getLogger(PipelineExecutor.class);
    private final Map<String, DataFrame> pipelineResults = new LinkedHashMap<>();
    private DataFrame lastResult;

    public PipelineExecutor(String startWith, DataFrame initialDataframe) {
        pipelineResults.put(startWith, initialDataframe);
        this.lastResult = initialDataframe;
    }

    public PipelineExecutor then(String calculate, String using, Function<DataFrame, DataFrame> step) {
        logger.info("execute {} using {}", calculate, using);
        DataFrame stepResult = step.apply(pipelineResults.get(using));
        pipelineResults.put(calculate, stepResult);
        lastResult = stepResult;
        return this;
    }

    public PipelineExecutor then(String calculate, Function<DataFrame, DataFrame> step) {
        logger.info("execute {}", calculate);
        DataFrame stepResult = step.apply(lastResult);
        pipelineResults.put(calculate, stepResult);
        lastResult = stepResult;
        return this;
    }

    public PipelineExecutor pass(Consumer<DataFrame> to) {
        logger.info("pass last result to consumer");
        to.accept(lastResult);
        return this;
    }

    public PipelineExecutor pass(String dataframe, Consumer<DataFrame> to) {
        logger.info("pass {} to consumer", dataframe);
        to.accept(pipelineResults.get(dataframe));
        return this;
    }

    public PipelineExecutor join(Collection<String> from, String to, Function<DataFrame[], DataFrame> joinOperation) {
        logger.info("join {} to {}", from, to);
        DataFrame stepResult = joinOperation.apply(MapUtil.getAllEntries(pipelineResults, from).values().toArray(new DataFrame[]{}));
        pipelineResults.put(to, stepResult);
        lastResult = stepResult;
        return this;
    }

    public Map<String, DataFrame> getPipelineResults() {
        return pipelineResults;
    }
}
