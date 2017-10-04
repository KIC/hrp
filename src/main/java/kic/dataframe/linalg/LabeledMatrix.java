package kic.dataframe.linalg;

import kic.dataframe.DataFrame;
import kic.utils.MapUtil;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.linear.RealMatrixChangingVisitor;
import org.apache.commons.math3.linear.RealMatrixPreservingVisitor;
import org.apache.commons.math3.stat.correlation.Covariance;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.DoubleFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by kindler on 16/09/2017.
 */
public class LabeledMatrix<RK, CK> {
    public final List<RK> rowLabels;
    public final List<CK> columnLabels;
    private final RealMatrix matrix;

    public LabeledMatrix(RK rowKey, Collection<CK> columns, double[] data) {
        this(Arrays.asList(rowKey), new ArrayList<>(columns), new double[][]{data});
    }

    public LabeledMatrix(List<RK> rowLabels, List<CK> columnLabels, double[][] matrix) {
        this(Collections.unmodifiableList(rowLabels),
             Collections.unmodifiableList(columnLabels),
             MatrixUtils.createRealMatrix(matrix));
    }

    protected LabeledMatrix(List<RK> rowLabels, List<CK> columnLabels, RealMatrix matrix) {
        if (rowLabels.size() != matrix.getRowDimension() || columnLabels.size() != matrix.getColumnDimension())
            throw new IllegalArgumentException("Dimensions do not match!");

        this.rowLabels = rowLabels;
        this.columnLabels = columnLabels;
        this.matrix = matrix;
    }

    public LabeledMatrix<RK, CK> getRow(int i) {
        return new LabeledMatrix<>(rowLabels.get(i), columnLabels, matrix.getRow(i));
    }

    public <CK2>Map<CK2, Double> getRowAsMap(int i, Function<? super CK, CK2> keyMapper) {
        return MapUtil.zip(columnLabels.stream().map(keyMapper).collect(Collectors.toList()),
                           Arrays.stream(matrix.getRow(i)).boxed().collect(Collectors.toList()));
    }

    public LabeledMatrix<CK, RK> getColumnAsRowVector(int j) {
        return new LabeledMatrix<>(columnLabels.get(j), rowLabels, matrix.getColumn(j));
    }

    public LabeledMatrix<CK, RK> transpose() {
        return new LabeledMatrix<>(columnLabels, rowLabels, matrix.transpose());
    }

    public <CK>LabeledMatrix<RK, CK> multiply(LabeledMatrix<?, CK> matrix2) {
        return new LabeledMatrix<>(rowLabels, matrix2.columnLabels, matrix.multiply(matrix2.matrix));
    }

    public LabeledMatrix<RK, CK> elementWise(LabeledMatrix other, Double2Operation operation) {
        // FIXME we want to merge on a label basis even if dimensions do not match

        RealMatrix copy = matrix.copy();
        copy.walkInOptimizedOrder(new RealMatrixChangingVisitor() {
            @Override
            public void start(int rows, int columns, int startRow, int endRow, int startColumn, int endColumn) { }

            @Override
            public double visit(int row, int column, double value) {
                return operation.calc(value, other.matrix.getEntry(row, column));
            }

            @Override
            public double end() {
                return 0;
            }
        });

        return new LabeledMatrix<>(rowLabels, columnLabels, copy);
    }

    public LabeledMatrix<RK, CK> elementWise(DoubleOperation operation) {
        RealMatrix copy = matrix.copy();
        copy.walkInOptimizedOrder(new RealMatrixChangingVisitor() {
            @Override
            public void start(int rows, int columns, int startRow, int endRow, int startColumn, int endColumn) { }

            @Override
            public double visit(int row, int column, double value) {
                return operation.calc(value);
            }

            @Override
            public double end() {
                return 0;
            }
        });

        return new LabeledMatrix<>(rowLabels, columnLabels, copy);
    }

    public double sum() {
        return matrix.walkInOptimizedOrder(new RealMatrixPreservingVisitor() {
            double sum = 0d;

            @Override
            public void start(int rows, int columns, int startRow, int endRow, int startColumn, int endColumn) { }

            @Override
            public void visit(int row, int column, double value) {
                sum += value;
            }

            @Override
            public double end() {
                return sum;
            }
        });
    }

    public LabeledMatrix getDiagonal(MatrixShape shape) {
        int l = Math.min(rowLabels.size(), columnLabels.size());

        if (shape == MatrixShape.ROW_VECTOR) {
            RealMatrix mx = MatrixUtils.createRealMatrix(1, l);
            for (int i = 0; i < l; i++) mx.setEntry(0, i, matrix.getEntry(i, i));
            return new LabeledMatrix(Arrays.asList("Diagonal"), columnLabels.subList(0, l), mx);
        } else if (shape == MatrixShape.COLUMN_VECTOR) {
            RealMatrix mx = MatrixUtils.createRealMatrix(l, 1);
            for (int i = 0; i < l; i++) mx.setEntry(i, 0, matrix.getEntry(i, i));
            return new LabeledMatrix(rowLabels.subList(0, l), Arrays.asList("Diagonal"), mx);
        } else if (shape == MatrixShape.ROW_MATRIX) {
            RealMatrix mx = MatrixUtils.createRealMatrix(l, l);
            for (int i = 0; i < l; i++) mx.setEntry(i, i, matrix.getEntry(i, i));
            return new LabeledMatrix(rowLabels.subList(0, l), columnLabels.subList(0, l), mx);
        } else {
            throw new IllegalArgumentException("invalid matrix shape");
        }
    }

    public LabeledMatrix<CK, CK> covariance(boolean deMean) {
        return new LabeledMatrix<>(columnLabels, columnLabels, new Covariance(matrix, !deMean).getCovarianceMatrix());
    }

    public LabeledMatrix<CK, CK> correlation(boolean deMean) {
        return new LabeledMatrix<>(columnLabels, columnLabels, new PearsonsCorrelation(new Covariance(matrix, !deMean)).getCorrelationMatrix());
    }

    public DataFrame<RK, CK, Double> toDataframe() {
        DataFrame<RK, CK, Double> df = new DataFrame<>();
        matrix.walkInOptimizedOrder(new RealMatrixPreservingVisitor() {
            @Override
            public void start(int rows, int columns, int startRow, int endRow, int startColumn, int endColumn) { }

            @Override
            public void visit(int row, int column, double value) {
                df.upsert(rowLabels.get(row), columnLabels.get(column), value);
            }

            @Override
            public double end() {
                return 0;
            }
        });

        return df;
    }

    public MinMax minMax() {
        AtomicReference<MinMax> result = new AtomicReference<>();
        matrix.walkInOptimizedOrder(new RealMatrixPreservingVisitor() {
            int minI = -1, minJ = -1, maxI = -1, maxJ = -1;
            double min = Double.MAX_VALUE, max = Double.MIN_VALUE;

            @Override
            public void start(int rows, int columns, int startRow, int endRow, int startColumn, int endColumn) {}

            @Override
            public void visit(int row, int column, double value) {
                if (value < min) {
                    min = value;
                    minI = row;
                    minJ = column;
                }

                if (value > max) {
                    max = value;
                    maxI = row;
                    maxJ = column;
                }
            }

            @Override
            public double end() {
                result.set(new MinMax(minI, minJ, maxI, maxJ, min, max));
                return 0;
            }
        });

        return result.get();
    }

    public Rescale rescale(double range0, double range1) {
        MinMax mm = minMax();
        return new Rescale(mm.min, mm.max, range0, range1);
    }

    public Rescale rescale(Object array) {
        if (array.getClass().isArray()) {
            MinMax mm = minMax();
            return new Rescale(mm.min, mm.max, array);
        } else  if (array instanceof Collection) {
            MinMax mm = minMax();
            return new Rescale(0,0, ((Collection) array).toArray());
        } else {
            throw new IllegalArgumentException("arguemnt needs to be collection or array");
        }
    }

    public double[][] to2DArray() {
        return matrix.getData();
    }

    public int rows() {
        return matrix.getRowDimension();
    }

    public int columns() {
        return matrix.getColumnDimension();
    }

    @Override
    public String toString() {
        return "LabeledMatrix{" +
                "rowLabels=" + rowLabels +
                ", columnLabels=" + columnLabels +
                ", matrix=" + matrix +
                '}';
    }

    public static class MinMax {
        public final int minI,minJ,maxI,maxJ;
        public final double min, max;

        public MinMax(int minI, int minJ, int maxI, int maxJ, double min, double max) {
            this.minI = minI;
            this.minJ = minJ;
            this.maxI = maxI;
            this.maxJ = maxJ;
            this.min = min;
            this.max = max;
        }
    }
}
