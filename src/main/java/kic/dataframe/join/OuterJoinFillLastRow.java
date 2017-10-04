package kic.dataframe.join;

import kic.dataframe.DataFrame;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.BiFunction;

public class OuterJoinFillLastRow<RK> implements JoinOperation<RK> {
    private final BiFunction<DataFrame<RK, ?, ?>, DataFrame<RK, ?, ?> , DataFrame<RK, ?, ?>> rightFunction;
    private DataFrame<RK, ?, ?> rightResult = new DataFrame<>();

    public OuterJoinFillLastRow() {
        this((l, r) -> r);
    }

    public OuterJoinFillLastRow(BiFunction<DataFrame<RK, ?, ?>, DataFrame<RK, ?, ?>, DataFrame<RK, ?, ?>> rightFunction) {
        this.rightFunction = rightFunction;
    }

    @Override
    public JoinedRow<RK> rowKey(RK baisKey, Optional<RK> preview, DataFrame baisDataFrame, PreviewIterator<RK> joinIt, DataFrame<RK, ?, ?> joinDataFrame) {
        DataFrame<RK, ?, ?> rres = rightFunction.apply(baisDataFrame, joinDataFrame.withRows(Arrays.asList(baisKey)));
        if (rres.rows() > 0) rightResult = rres;

        // this basically means we never have a null but only an empty data frame on missing data
        return new JoinedRow<>(baisKey, rightResult);
    }

}
