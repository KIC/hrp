package kic.dataframe

import spock.lang.Specification

/**
 * Created by kindler on 17/09/2017.
 */
class VisitorTest extends Specification {
    def "test walkRowWise"() {
        given:
        DataFrame df = new TestDataFrames().integers()

        when:
        def list = df.select(columns)
                     .withRows(rows)
                     .visit.walkRowWise([], { rk, res -> _}, { ck, val, res -> if (val != null) res << val});

        then:
        list == expected

        where:
        rows                     | columns              | expected
        [0L, 1L, 2L, 3L, 4L, 5L] | ["A", "B", "C", "D"] | [0, 1, 2, 3, 10, 11, 12, 13, 20, 21, 22, 23, 30, 31, 32, 33, 40, 41, 42, 50, 51, 52, 53]
        [        2L,     4L,   ] | ["A",      "C"     ] | [20, 22, 40, 42]
        [        2L,     4L,   ] | [          "C"     ] | [22, 42]
    }
}
