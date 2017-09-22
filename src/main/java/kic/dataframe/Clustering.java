package kic.dataframe;

import com.apporiented.algorithm.clustering.Cluster;
import com.apporiented.algorithm.clustering.ClusteringAlgorithm;
import com.apporiented.algorithm.clustering.DefaultClusteringAlgorithm;
import com.apporiented.algorithm.clustering.SingleLinkageStrategy;
import kic.interfaces.ToDouble;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Clustering<RK, CK, V> {
    private final DataFrame<RK, CK, V> dataFrame;
    private final ToDouble<V> toDistance;

    public Clustering(DataFrame<RK, CK, V> dataFrame, ToDouble<V> toDistance) {
        this.dataFrame = dataFrame;
        this.toDistance = toDistance;
    }

    public DataFrame<RK, CK, V> cluster() {
         return cluster(new DefaultClusteringAlgorithm());
    }

    public DataFrame<RK, CK, V> cluster(ClusteringAlgorithm alg) {
        LinkedHashMap<String, CK> colIndexMap = new LinkedHashMap<>();
        for (CK ck : dataFrame.getColumnOrder()) colIndexMap.put(ck.toString(), ck);

        Cluster cluster = alg.performClustering(
                dataFrame.toMatrix(false, toDistance).to2DArray(),
                colIndexMap.keySet().toArray(new String[0]),
                new SingleLinkageStrategy());

        // get ordered columns
        List<CK> orderedColumns = getSortedNames(cluster).stream().map(n -> colIndexMap.get(n)).collect(Collectors.toList());

        // if rows and columns are equal make them symetric automatically
        if (dataFrame.getRowOrder().size() == orderedColumns.size() && dataFrame.getRowOrder().containsAll(orderedColumns)) {
            return new DataFrame<>(dataFrame, (List<RK>) orderedColumns, orderedColumns);
        } else {
            return new DataFrame<>(dataFrame, dataFrame.getRowOrder(), orderedColumns);
        }
    }

    private List<String> getSortedNames(Cluster cluster) {
        List<String> result = new ArrayList<>();

        for (Cluster c : cluster.getChildren()) {
            if (c.isLeaf()) {
                result.add(c.getName());
            } else {
                result.addAll(getSortedNames(c));
            }
        }

        return result;
    }


}
