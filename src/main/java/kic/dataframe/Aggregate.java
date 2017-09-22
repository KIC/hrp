package kic.dataframe;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by kindler on 15/09/2017.
 */
public class Aggregate<RK, CK, V> {
    private final DataFrame<RK, CK, V> dataFrame;

    public Aggregate(DataFrame<RK, CK, V> dataFrame) {
        this.dataFrame = dataFrame;
    }

    public <R>Map<CK, R> aggregateRows(Function<List<V>, R> aggregator) {
        /*getColumnOrder().stream()
                .collect(Collectors.toMap(
                        ck -> ck,
                        ck -> aggregator.apply(columnRowIndex.get(ck)
                                .values()
                                .stream()
                                .map(i -> data.get(i))
                                .collect(Collectors.toList()))));
                                */
        throw new UnsupportedOperationException("not supported");
    }

}
