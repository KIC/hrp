package kic.dataframe

import spock.lang.Specification


class ClusteringTest extends Specification {
    def "test cluster"() {
        given:
        DataFrame df = new TestDataFrames().symetricDoubles()

        when:
        def clustered = df.clustering().cluster()

        then:
        clustered.getColumnOrder() == ["O6" ,"O3", "O5", "O4", "O1", "O2"]
        clustered.getRowOrder() == ["O6" ,"O3", "O5", "O4", "O1", "O2"]
    }
}
