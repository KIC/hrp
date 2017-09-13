package kic.dataframe;

import org.apache.commons.math3.linear.DefaultRealMatrixChangingVisitor;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Function;

public class Linalg {

    public static class Matrix<RK, CK> {
        private final List<RK> rowLabels;
        private final List<CK> columnLabels;
        private final RealMatrix matrix;

        public Matrix(Collection<RK> rowLabels, Collection<CK> columnLabels, RealMatrix matrix) {
            this.rowLabels = new ArrayList<>(new LinkedHashSet<>(rowLabels));
            this.columnLabels = new ArrayList<>(new LinkedHashSet<>(columnLabels));

            if (matrix.getRowDimension() != rowLabels.size()) {
                throw new IllegalArgumentException("row dimension does not match");
            } else if (matrix.getColumnDimension() != columnLabels.size()) {
                throw new IllegalArgumentException("column dimension does not match");
            } else {
                this.matrix = matrix;
            }
        }

        public Matrix<String, CK> getDiagonal() {
            if (!columnLabels.equals(rowLabels)) throw new IllegalStateException("Matrix not symetric");
            double[] diag = new double[Math.min(matrix.getRowDimension(), matrix.getColumnDimension())];
            for (int i=0; i<diag.length; i++) diag[i] = matrix.getEntry(i,i);

            return new Matrix<>(Arrays.asList("diagonal"), columnLabels, MatrixUtils.createRealMatrix(new double[][]{diag}));
        }

        public Matrix<RK, CK> map(Function<Double, Double> elementWiseOperation) {
            RealMatrix copy = matrix.copy();
            copy.walkInOptimizedOrder(new DefaultRealMatrixChangingVisitor(){
                @Override
                public double visit(int row, int column, double value) {
                    return elementWiseOperation.apply(value);
                }
            });

            return new Matrix<>(rowLabels, columnLabels, copy);
        }

        public double sum() {
            return Arrays.stream(matrix.getData()).mapToDouble(r -> Arrays.stream(r).sum()).sum();
        }
    }

    public static <RK, CK, V>double calcScalar(DataFrame<RK, CK, V> dataFrame, Function<V, Double> toDouble, Function<Matrix<RK, CK>, Double> matrixOperation) {
        RealMatrix realMatrix = MatrixUtils.createRealMatrix(dataFrame.asDoubleMatrix(toDouble, false));
        return matrixOperation.apply(new Matrix<>(dataFrame.getRowOrder(), dataFrame.getColumnOrder(), realMatrix));
    }

    public static <RK, CK, V> DataFrame<RK, CK, Double> calc(DataFrame<RK, CK, V> dataFrame, Function<V, Double> toDouble, Function<Matrix<RK, CK>, Matrix<RK, CK>> matrixOperation) {
        RealMatrix realMatrix = MatrixUtils.createRealMatrix(dataFrame.asDoubleMatrix(toDouble, false));
        Matrix<RK, CK> resultMatrix = matrixOperation.apply(new Matrix<>(dataFrame.getRowOrder(), dataFrame.getColumnOrder(), realMatrix));
        DataFrame<RK, CK, Double> result = new DataFrame<>();
        int i=0, j=0;

        for (RK rk : resultMatrix.rowLabels) {
            j = 0;
            for (CK ck : resultMatrix.columnLabels) {
                result.upsert(rk, ck, resultMatrix.matrix.getEntry(i, j));
                j++;
            }
            i++;
        }

        return result;
    }

}
