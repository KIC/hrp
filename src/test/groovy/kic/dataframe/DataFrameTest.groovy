package kic.dataframe

import spock.lang.Specification

/**
 * Created by kindler on 16/09/2017.
 */
class DataFrameTest extends Specification {
    def "test upsert"() {
        given:
        def testFrames = new TestDataFrames()
        DataFrame df = testFrames.integers()

        when:
        df.upsert(rk, ck, val)

        then:
        df.data == expected
        df.rows() == testFrames.rows.size()
        df.columns() == testFrames.columns.size()
        df.getElement(rk, ck) == val
        df.getRowOrder() == testFrames.rows
        df.getColumnOrder() == testFrames.columns

        where:
        rk | ck  | val | expected
        2L | "B" | 21  | [0, 1, 2, 3, 10, 11, 12, 13, 20, 21, 22, 23, 30, 31, 32, 33, 40, 41, 42, 50, 51, 52, 53]
        4L | "D" | 99  | [0, 1, 2, 3, 10, 11, 12, 13, 20, 21, 22, 23, 30, 31, 32, 33, 40, 41, 42, 50, 51, 52, 53, 99]
    }

    def "test transpose"() {
        given:
        def testFrames = new TestDataFrames()
        DataFrame df = testFrames.integers()

        when:
        def t = df.transpose()

        then:
        t.reshape.toMapOfRowMaps() == df.reshape.toMapOfColumMaps()
    }


    def "test select"() {
        given:
        def testFrames = new TestDataFrames()
        DataFrame df = testFrames.integers()

        when:
        def selectedDataFrame = df.select(select, nulls)
        def selectedMaps = selectedDataFrame.reshape.toMapOfRowMaps()

        then:
        selectedMaps.size() == expected.size()
        selectedMaps.each { it.value == expected[it.key] }
        selectedDataFrame.select(["A"]).reshape.toMapOfRowMaps().each { it.value == [:] }

        where:
        select     | nulls | expected
        ["B", "D"] | false | [0:[B:1, D:3], 1:[B:11, D:13], 2:[B:21, D:23], 3:[B:31, D:33], 4:[B:41, D:null], 5:[B:51, D:53]]
        ["B", "F"] | false | [0:[B:1], 1:[B:11], 2:[B:21], 3:[B:31], 4:[B:41], 5:[B:51]]
        ["B", "F"] | true  | [0:[B:1, F:null], 1:[B:11, F:null], 2:[B:21, F:null], 3:[B:31, F:null], 4:[B:41, F:null], 5:[B:51, F:null]]
    }

    def "test withRow"() {

    }

    def "test getElement"() {
        def testFrames = new TestDataFrames()
        DataFrame df = testFrames.integers()

        when:
        def a = df.select(columns).withRows(rows)

        then:
        a.getElement(row, column) == expected

        where:
        row | column | columns    | rows                     | expected
        2L  | "B"    | ["B", "D"] | [0L, 1L, 2L, 3L, 4L, 5L] | 21
        4L  | "D"    | ["B", "D"] | [0L, 1L, 2L, 3L, 4L, 5L] | null
        1L  | "A"    | ["B", "D"] | [0L, 1L, 2L, 3L, 4L, 5L] | null
        2L  | "B"    | ["B", "D"] | [0L, 1L,     3L, 4L, 5L] | null
    }

    def "test map"() {
        given:
        def testFrames = new TestDataFrames()
        DataFrame df = testFrames.integers()

        when:
        def ddf = df.map({i -> 1d/i})

        then:
        ddf.data.count({ it.class != java.lang.Double.class }) == 0
    }

}
