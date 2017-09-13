package kic.dataframe;

import com.apporiented.algorithm.clustering.Cluster;
import com.apporiented.algorithm.clustering.ClusteringAlgorithm;
import com.apporiented.algorithm.clustering.DefaultClusteringAlgorithm;
import com.apporiented.algorithm.clustering.SingleLinkageStrategy;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DataFrame<RK, CK, V> implements Serializable {
    private static final Map EMPTY_MAP = Collections.unmodifiableMap(new HashMap());
    private final List<RK> rowOrder;
    private final List<CK> columnOrder;
    private final Map<RK, Map<CK, Integer>> rowColumnIndex; // never loop over the index only use rowOrder
    private final Map<CK, Map<RK, Integer>> columnRowIndex; // never loop over the index only use columnOrder
    private final List<V> data;

    public static class Tuple<RK, V> {
        public final RK rowKey;
        public final V value;

        public Tuple(RK rowKey, V value) {
            this.rowKey = rowKey;
            this.value = value;
        }
    }

    public DataFrame() {
        this.rowOrder = new ArrayList<>();
        this.columnOrder = new ArrayList<>();
        this.rowColumnIndex = new HashMap<>();
        this.columnRowIndex = new HashMap<>();
        this.data = new ArrayList<>();
    }

    private DataFrame(DataFrame source, List<RK> rowOrder, List<CK> columnOrder) {
        this.rowOrder = rowOrder;
        this.columnOrder = columnOrder;
        this.rowColumnIndex = source.rowColumnIndex;
        this.columnRowIndex = source.columnRowIndex;
        this.data = source.data;
    }

    public synchronized void upsert(RK rowKey, CK columKey, V value) {
        Map<CK, Integer> columnIndex = rowColumnIndex.getOrDefault(rowKey, EMPTY_MAP);
        Map<RK, Integer> rowIndex = columnRowIndex.getOrDefault(columKey, EMPTY_MAP);
        Integer col = columnIndex.get(columKey);
        Integer row = rowIndex.get(rowKey);

        if (row != null && col != null) {
            // update
            data.set(columnIndex.get(columKey), value);
        } else {
            // insert
            int idx = data.size();
            data.add(value);
            rowColumnIndex.computeIfAbsent(rowKey, r -> {rowOrder.add(r); return new HashMap<>();}).put(columKey, idx);
            columnRowIndex.computeIfAbsent(columKey, c -> {columnOrder.add(c); return new HashMap<>();}).put(rowKey, idx);
        }
    }

    /*public DataFrame<RK, CK, V> withColumnOrder(Iterable<CK> orderedColumns) {
        return this;
    }*/

    public List<RK> getRowOrder() {
        return Collections.unmodifiableList(rowOrder);
    }

    public List<CK> getColumnOrder() {
        return Collections.unmodifiableList(columnOrder);
    }

    public Set<CK> getFirstRowAndLastRowsColumnsIntersect() {
        Set<CK> intersect = new LinkedHashSet<>(getFirstRow().value.keySet());
        intersect.retainAll(getLastRow().value.keySet());
        return intersect;
    }

    public int rows() {
        return rowOrder.size();
    }

    public int columns() {
        return columnOrder.size();
    }

    public Tuple<RK, LinkedHashMap<CK,V>> getFirstRow() {
        RK rk = rowOrder.get(0);
        return new Tuple<>(rk, getRow(rk));
    }

    public Tuple<RK, LinkedHashMap<CK,V>> getLastRow() {
        RK rk = rowOrder.get(rows()-1);
        return new Tuple<>(rk, getRow(rk));
    }

    public LinkedHashMap<CK,V> getRow(RK rowKey) {
        LinkedHashMap<CK,V> row = new LinkedHashMap<>();
        Map<CK, Integer> columnIndex = rowColumnIndex.get(rowKey);
        for (CK ck : columnIndex.keySet()) {
            row.put(ck, data.get(columnIndex.get(ck)));
        }

        return row;
    }


    public DataFrame<RK, CK, V> withRowOrdering(Collection<RK> rows) {
        return new DataFrame<>(this, new ArrayList<>(new LinkedHashSet<>(rows)), columnOrder);
    }

    public DataFrame<RK, CK, V> select(Collection<CK> columns, Collection<RK> rows) {
        return new DataFrame<>(this,
                new ArrayList<>(new LinkedHashSet<>(rows)),
                new ArrayList<>(new LinkedHashSet<>(columns)));
    }

    public DataFrame<RK, CK, V> select(Collection<CK> columns) {
        return select(columns, getRowOrder());
    }

    public <RK2,CK2,V2>DataFrame<RK2, CK2, V2> slide(int windowSize, BiConsumer<DataFrame<RK,CK,V>, DataFrame<RK2,CK2,V2>> windowFunction) {
        /* we could make a window function of 60, which is calculating the 59 returns and then calculates the correlcation/covariance matrices and put those in a new DataFrame
            in the window function i would pass the same object but with different rowOrderings

            also zuerst haben wir preise
            returns = prices.slide(2, calcReturns)
            covcor = returns.slide(60, calcCovarianceAndCorrleation) // DataFrame<RK,CK,DataFrame>
            clusteredCov = covcorr.slide(1, clusterCov)

            weights = clusteredCov.slide(1, hrp)
            portfolioReturns = weights' * returns
            portfolioRIsk = weights' * cov * weights
         */
        DataFrame<RK2,CK2,V2> result = new DataFrame<>();
        List<RK> rowKeys = new LinkedList<>();
        for (RK rk : rowOrder) {
            rowKeys.add(rk);
            while (rowKeys.size() > windowSize) {
                Iterator<RK> it = rowKeys.iterator();
                it.next();
                it.remove();
            }

            if (rowKeys.size() >= windowSize) {
                DataFrame<RK, CK, V> window = new DataFrame<>(this, rowKeys, columnOrder);
                windowFunction.accept(window, result);
            }
        }

        return result;
    }

    public void aggregateRows() {
        // what to do here ...
        // what should we return? LinkedMap<RK, V>
    }

    public DataFrame<RK, CK, V> cluster(Function<V, Double> toDistance) {
        LinkedHashMap<String, CK> colIndexMap = new LinkedHashMap<>();
        for (CK ck : columnOrder) colIndexMap.put(ck.toString(), ck);

        ClusteringAlgorithm alg = new DefaultClusteringAlgorithm();
        Cluster cluster = alg.performClustering(
                asDoubleMatrix(toDistance, false),
                colIndexMap.keySet().toArray(new String[0]),
                new SingleLinkageStrategy());

        List<CK> orderedColumns = Clustering.getSortedNames(cluster).stream().map(n -> colIndexMap.get(n)).collect(Collectors.toList());
        return new DataFrame<>(this, rowOrder, orderedColumns);
    }

    public DataFrame<CK, CK, V> clusterSymetric(Function<V, Double> toDistance) {
        if (!rowOrder.equals(columnOrder)) throw new IllegalStateException("Data is not symetric! " + rowOrder + " vs " + columnOrder);
        DataFrame<RK, CK, V> clustered = cluster(toDistance);
        return (DataFrame<CK, CK, V>) clustered.withRowOrdering((List<RK>) clustered.getColumnOrder());
    }

    public double[][] asDoubleMatrix(Function<V, Double> toDouble, boolean useLastIfMissing) {
        double[][] mx = new double[rows()][columns()];
        int i=0, j=0;
        V v = null;

        for (RK rk : rowOrder) {
            j=0;
            for (CK ck : columnOrder) {
                v = getDataOr(rowColumnIndex.get(rk).get(ck), null);
                if (v != null) {
                    mx[i][j] = toDouble.apply(v);
                } else if (useLastIfMissing && i > 0) {
                    mx[i][j] = mx[i-1][j];
                }
                j++;
            }
            i++;
        }

        return mx;
    }

    private V getDataOr(Integer idx, V or) {
        if (idx != null) {
            return data.get(idx);
        } else {
            return or;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        // header
        sb.append("ROW, ");
        for (CK ck : columnOrder) sb.append(ck).append(", ");
        sb.append("\n");

        // body
        for (RK rk : rowOrder) {
            Map<CK, Integer> columnIndex = rowColumnIndex.get(rk);
            sb.append(rk).append(", ");
            for (CK ck : columnOrder) {
                sb.append(getDataOr(columnIndex.get(ck), null)).append(", ");
            }
            sb.append("\n");
        }

        return sb.toString();
    }

}
