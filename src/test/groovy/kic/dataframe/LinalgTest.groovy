package kic.dataframe

import kic.dataframe.linalg.LabeledMatrix
import spock.lang.Specification

/**
 * Created by kindler on 17/09/2017.
 */
class LinalgTest extends Specification {

    def "test toMatrix"() {
        given:
        DataFrame df = new TestDataFrames().integers()

        when:
        LabeledMatrix mx = df.toMatrix(fill, {Long l -> l.toDouble()})

        then:
        mx.to2DArray() == expected

        where:
        fill    | expected
        false   | [[0.0, 1.0, 2.0, 3.0], [10.0, 11.0, 12.0, 13.0], [20.0, 21.0, 22.0, 23.0], [30.0, 31.0, 32.0, 33.0], [40.0, 41.0, 42.0, 0.0], [50.0, 51.0, 52.0, 53.0]]
        true    | [[0.0, 1.0, 2.0, 3.0], [10.0, 11.0, 12.0, 13.0], [20.0, 21.0, 22.0, 23.0], [30.0, 31.0, 32.0, 33.0], [40.0, 41.0, 42.0, 33.0], [50.0, 51.0, 52.0, 53.0]]
    }

    def "test toMatrix with selection and nulls"() {
        given:
        DataFrame df = new TestDataFrames().integers().select(["A", "F"], nulls)

        when:
        LabeledMatrix mx = df.toMatrix {Long l -> l.toDouble()}

        then:
        mx.to2DArray() == expected

        where:
        nulls    | expected
        false    | [[0.0], [10.0], [20.0], [30.0], [40.0], [50.0]]
        true     | [[0.0, 0.0], [10.0, 0.0], [20.0, 0.0], [30.0, 0.0], [40.0, 0.0], [50.0, 0.0]]
    }

}
