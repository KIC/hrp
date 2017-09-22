package kic.dataframe;

import kic.dataframe.linalg.LabeledMatrix;
import kic.interfaces.ToDouble;
import kic.utils.MapUtil;

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
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class DataFrame<RK, CK, V> implements Serializable {
    private static final Map EMPTY_MAP = Collections.unmodifiableMap(new HashMap());
    private final List<RK> rowOrder;
    private final List<CK> columnOrder;
    private final Map<RK, Map<CK, Integer>> rowColumnIndex; // never loop over the index only use rowOrder
    private final Map<CK, Map<RK, Integer>> columnRowIndex; // never loop over the index only use columnOrder
    private final List<V> data;
    // In Scala word we would use implicit classes for composition but in java we have to hardwire
    public final Reshape<RK, CK, V> reshape = new Reshape<>(this);
    public final Visitor<RK, CK, V> visit = new Visitor<>(this);

    public DataFrame() {
        this.rowOrder = new ArrayList<>();
        this.columnOrder = new ArrayList<>();
        this.rowColumnIndex = new HashMap<>();
        this.columnRowIndex = new HashMap<>();
        this.data = new ArrayList<>();
    }

    protected DataFrame(DataFrame source, List<RK> rowOrder, List<CK> columnOrder) {
        this(source, rowOrder, columnOrder, source.data);
    }

    private DataFrame(DataFrame source, List<RK> rowOrder, List<CK> columnOrder, List<V> data) {
        this(source, rowOrder, columnOrder, source.rowColumnIndex, source.columnRowIndex, data);
    }

    private DataFrame(DataFrame source, List<RK> rowOrder, List<CK> columnOrder, Map<RK, Map<CK, Integer>> rowColumnIndex, Map<CK, Map<RK, Integer>> columnRowIndex, List<V> data) {
        this.rowOrder = rowOrder;
        this.columnOrder = columnOrder;
        // FIXME either make the indices unmodifyable (and also the submaps) or keep them unchanged to allow a correct "upsert"
        this.rowColumnIndex = MapUtil.getAllEntries(rowColumnIndex, rowOrder);
        this.columnRowIndex = MapUtil.getAllEntries(columnRowIndex, columnOrder);
        this.data = data;
    }


    public synchronized void upsert(RK rowKey, CK columKey, V value) {
        if (value == null) return; // do not store null values
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


    // TODO move to a different place
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

    public DataFrame<CK, RK, V> transpose() {
        return new DataFrame<>(this, columnOrder, rowOrder, columnRowIndex, rowColumnIndex, data);
    }

    public DataFrame<RK, CK, V> select(Predicate<? super CK> filter) {
        return select(columnOrder.stream().filter(filter).collect(Collectors.toList()), false);
    }

    public DataFrame<RK, CK, V> select(Collection<CK> columns) {
        return select(columns, false);
    }

    public DataFrame<RK, CK, V> select(Collection<CK> columns, boolean withNulls) {
        List<CK> cols = new ArrayList<>(new LinkedHashSet<CK>(columns));
        if (!withNulls) cols.retainAll(columnOrder);
        return new DataFrame<>(this, rowOrder, cols);
    }

    public DataFrame<RK, CK, V> withRows(Collection<RK> rows) {
        return new DataFrame<>(this, new ArrayList<>(new LinkedHashSet<>(rows)), columnOrder);
    }

    public <R>DataFrame<RK, CK, R> map(Function<V, R> function) {
        // Note this is actually wrong we only want to transform what is in rowOrdered/columnOrdered
        // however the result is correct alltough its not very memory efficient for small subsets of large dataframes
        return new DataFrame<>(this, rowOrder, columnOrder, data.stream().map(function).collect(Collectors.toList()));
    }

    public DataFrame<RK, CK, V> sortRows() {
        return withRows(new TreeSet<>(rowOrder));
    }

    public DataFrame<RK, CK, V> sortColumns() {
        return select(new TreeSet<>(columnOrder));
    }

    public LabeledMatrix<RK, CK> toMatrix() {
        return toMatrix(false);
    }

    public LabeledMatrix<RK, CK> toMatrix(boolean fillMissingWithLast) {
        return toMatrix(fillMissingWithLast, ToDouble.identity);
    }

    public LabeledMatrix<RK, CK> toMatrix(ToDouble<V> toDouble) {
        return toMatrix(false, toDouble);
    }

    public LabeledMatrix<RK, CK> toMatrix(boolean fillMissingWithLast, ToDouble<V> toDouble) {
        return new Linalg<>(this, toDouble).toMatrix(fillMissingWithLast);
    }

    public Clustering<RK, CK, V> clustering() {
        return clustering(d -> (Double) d);
    }

    public Clustering<RK, CK, V> clustering(ToDouble<V> distance) {
        return new Clustering<>(this, distance);
    }

    public RK firstRowKey() {
        return rowOrder.size() > 0 ? rowOrder.get(0) : null;
    }

    public RK lastRowKey() {
        return rowOrder.size() > 0 ? rowOrder.get(rows()-1) : null;
    }

    public V getElement(RK rk, CK ck) {
        return columnRowIndex.containsKey(ck)
            ? getDataOr((Integer) rowColumnIndex.getOrDefault(rk, EMPTY_MAP).get(ck), null)
            : null;
    }

    public V getDataOr(Integer idx, V or) {
        return idx != null? data.get(idx): or;
    }

    public List<RK> getRowOrder() {
        return Collections.unmodifiableList(rowOrder);
    }

    public List<CK> getColumnOrder() {
        return Collections.unmodifiableList(columnOrder);
    }

    public int rows() {
        return rowOrder.size();
    }

    public int columns() {
        return columnOrder.size();
    }

    public Map<CK, Integer> countStar() {
        return columnOrder.stream()
                .collect(Collectors.toMap(ck -> ck, ck -> MapUtil.getAllEntries(columnRowIndex.get(ck), rowOrder).size()));
    }

    public Collection<V> getData() {
        return Collections.unmodifiableCollection(data);
    }

    @Override
    public String toString() {
        return reshape.toString();
    }

}
