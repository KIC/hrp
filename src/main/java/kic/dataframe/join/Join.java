package kic.dataframe.join;

import kic.dataframe.DataFrame;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * Idee: wir machen ein left/right join so, dass wir die basis (z.b.) left 1:1 übertragen
 * Dann ergänzen wir die values aus der joinOLD funktion (outer joinOLD)
 * Und zuletzt filtern wir die ganze liste anhand der joinfunktion (inner join)
 *
 * TODO eventaully we also offer a right and a full join method
 */
public class Join<RK>  {
    private final DataFrame<RK, String, DataFrame> joinedDF = new DataFrame<>();
    private String baisDataFrameName = "";

    public Join(DataFrame<RK, Object, ?> basis, String as) {
        this(basis, as, Function.identity());
    }

    public Join(DataFrame<RK, Object, ?> basis, String as, Function<DataFrame<RK, Object, ?>, DataFrame<RK, Object, ?>> extractor) {
        baisDataFrameName = as;

        // first we put the join base in place, this is fine for outer joins, we just need to filter afterwards for inner joins
        for (RK rk : basis.getRowOrder()) {
            joinedDF.upsert(rk, as, extractor.apply(basis.withRows(Arrays.asList(rk))));
        }
    }


    // TODO Hmmmm ... eventually we can join the full dataframes instead of the single values!
    // probably its enough we passOverTo o number of dataframes in and give
    // foo.left("FOO").join(bar, "BAR", new LeftOuterFillLastRow()) // how to proceed to not get kind of a tree ... more like a builder

    public Join<RK> left(DataFrame<RK, ?, ?> right, String as, JoinOperation<RK> joinOperation) {
        PreviewIterator<RK> leftIt = new PreviewIterator(joinedDF.getRowOrder().iterator());
        PreviewIterator<RK> rightIt = new PreviewIterator<>(right.getRowOrder().iterator());
        List<RK> targetRows = new ArrayList<>();

        while (leftIt.hasNext()) {
            RK leftKey = leftIt.next();
            JoinedRow<RK> row = joinOperation.rowKey(leftKey, leftIt.getPreview(), joinedDF.getElement(leftKey, baisDataFrameName), rightIt, right);

            if (row != null) {
                targetRows.add(row.rowKey);
                joinedDF.upsert(row.rowKey, as, row.joinData);
            }

            // finally we filter rows excluded by the join function (inner joins)
            joinedDF.withRows(targetRows);
        }

        return this;
    }


    public DataFrame<RK, String, DataFrame> getJoinedDataFrame() {
        return joinedDF;
    }
}
