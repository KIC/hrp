package kic.dataframe;

import kic.dataframe.linalg.LabeledMatrix;
import kic.interfaces.ToDouble;
import org.apache.commons.math3.linear.DefaultRealMatrixChangingVisitor;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Function;

public class Linalg<RK, CK, V> {
    private final DataFrame<RK, CK, V> dataFrame;
    private final ToDouble<V> convert;

    public Linalg(DataFrame<RK, CK, V> dataFrame, ToDouble<V> convert) {
        this.dataFrame = dataFrame;
        this.convert = convert;
    }

    public LabeledMatrix<RK, CK> toMatrix() {
        return toMatrix(false);
    }

    public LabeledMatrix<RK, CK> toMatrix(boolean useLastRowsValueIfMissing) {
        return new LabeledMatrix<>(
                dataFrame.getRowOrder(),
                dataFrame.getColumnOrder(),
                dataFrame.visit.walkRowWise(
                        new DoubleArrayVisitor(dataFrame.rows(), dataFrame.columns()),
                        (rk, result) -> {result.i++; result.j=0;},
                        (ck, value, result) -> result.data[result.i][result.j++] = value == null
                                ? result.i > 0 && useLastRowsValueIfMissing ? result.data[result.i - 1][result.j - 1] : 0d
                                : convert.toDouble(value)

                ).data);
    }

    private static class DoubleArrayVisitor {
        int i = -1;
        int j = 0;
        final double[][] data;

        public DoubleArrayVisitor(int rows, int cols) {
            this.data = new double[rows][cols];
        }
    }

}
