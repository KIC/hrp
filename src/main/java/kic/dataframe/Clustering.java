package kic.dataframe;

import com.apporiented.algorithm.clustering.Cluster;

import java.util.ArrayList;
import java.util.List;

public class Clustering {

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
