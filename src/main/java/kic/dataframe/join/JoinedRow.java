package kic.dataframe.join;

import kic.dataframe.DataFrame;

/**
 * Created by kindler on 26/09/2017.
 */
public class JoinedRow<RK> {
    public final RK rowKey;
    public final DataFrame<RK, ?, ?> joinData;

    public JoinedRow(RK rowKey, DataFrame<RK, ?, ?> joinData) {
        this.rowKey = rowKey;
        this.joinData = joinData;
    }
}
