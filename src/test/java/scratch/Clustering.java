package scratch;

import com.apporiented.algorithm.clustering.Cluster;
import com.apporiented.algorithm.clustering.ClusteringAlgorithm;
import com.apporiented.algorithm.clustering.DefaultClusteringAlgorithm;
import com.apporiented.algorithm.clustering.SingleLinkageStrategy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Created by kindler on 11/09/2017.
 */
public class Clustering {

    public static void main(String[] args) {
        String[] names = new String[] { "O1", "O2", "O3", "O4", "O5", "O6" };
        double[][] distances = new double[][] {
                { 0, 1, 9, 7, 11, 14 },
                { 1, 0, 4, 3, 8, 10 },
                { 9, 4, 0, 9, 2, 8 },
                { 7, 3, 9, 0, 6, 13 },
                { 11, 8, 2, 6, 0, 10 },
                { 14, 10, 8, 13, 10, 0 }};

        ClusteringAlgorithm alg = new DefaultClusteringAlgorithm();
        Cluster cluster = alg.performClustering(distances, names, new SingleLinkageStrategy());

        cluster.toConsole(4);


        System.out.println(getSortedNames(cluster));
    }

    public static List<String> getSortedNames(Cluster cluster) {
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
