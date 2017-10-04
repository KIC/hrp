package kic.dataframe.join;

import kic.dataframe.DataFrame;

import java.util.Optional;

/**
 * Created by kindler on 25/09/2017.
 */
public interface JoinOperation<RK> {
    JoinedRow<RK> rowKey(RK leftKey, Optional<RK> preview, DataFrame baisDataFrame, PreviewIterator<RK> rightIt, DataFrame<RK, ?, ?> joinDataFrame);
}
