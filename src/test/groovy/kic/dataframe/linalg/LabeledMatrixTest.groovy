package kic.dataframe.linalg

import kic.dataframe.DataFrame
import kic.dataframe.TestDataFrames
import spock.lang.Specification

/**
 * Created by kindler on 17/09/2017.
 */
class LabeledMatrixTest extends Specification {

    def "test transpose"() {
        given:
        LabeledMatrix mx = new TestDataFrames().tallDoubles().toMatrix()

        when:
        def t = mx.transpose()

        then:
        mx.to2DArray() == [[0.0, 0.5, 1.0], [1.0, 1.5, 2.0], [2.0, 2.5, 3.0], [3.0, 3.5, 4.0], [4.0, 4.5, 5.0]]
        t.to2DArray() == [[0.0, 1.0, 2.0, 3.0, 4.0], [0.5, 1.5, 2.5, 3.5, 4.5], [1.0, 2.0, 3.0, 4.0, 5.0]]
        t.columnLabels == mx.rowLabels
        t.rowLabels == mx.columnLabels
    }

    def "test elementWise"() {
        given:
        LabeledMatrix mx = new TestDataFrames().tallDoubles().toMatrix()

        when:
        def added = mx.elementWise({d -> d + 10d});

        then:
        added.to2DArray() == [[10.0, 10.5, 11.0], [11.0, 11.5, 12.0], [12.0, 12.5, 13.0], [13.0, 13.5, 14.0], [14.0, 14.5, 15.0]]
    }

    def "test getDiagonal"() {
        given:
        LabeledMatrix mx = new TestDataFrames().tallDoubles().toMatrix()

        when:
        def diagonalR = mx.getDiagonal(MatrixShape.ROW_VECTOR)
        def diagonalC = mx.getDiagonal(MatrixShape.COLUMN_VECTOR)
        def diagonalM = mx.getDiagonal(MatrixShape.ROW_MATRIX)

        then:
        diagonalR.to2DArray() == [[0.0, 1.5, 3.0]]
        diagonalC.to2DArray() == [[0.0], [1.5], [3.0]]
        diagonalM.to2DArray() == [[0.0, 0.0, 0.0], [0.0, 1.5, 0.0], [0.0, 0.0, 3.0]]
    }


    def "test multiply"() {
        given:
        LabeledMatrix mx = new TestDataFrames().tallDoubles().toMatrix()

        when:
        def diagonalR = mx.getDiagonal(MatrixShape.ROW_VECTOR)
        def symetric = mx.transpose().multiply(mx)
        def r = diagonalR.multiply(symetric)

        then:
        symetric.columnLabels == ["A", "B", "C"]
        symetric.rowLabels == ["A", "B", "C"]
        r.columnLabels == ["A", "B", "C"]
        r.rowLabels == ["Diagonal"]
        r.to2DArray() == [[172.5,204.375,236.25]]
    }

    def "test sum"() {
        given:
        LabeledMatrix mx = new TestDataFrames().tallDoubles().toMatrix()

        when:
        def diagonalR = mx.getDiagonal(MatrixShape.ROW_VECTOR)
        def diagonalC = mx.getDiagonal(MatrixShape.COLUMN_VECTOR)
        def mxScalar = diagonalR.multiply(mx.transpose().multiply(mx)).multiply(diagonalC)

        then:
        mx.sum() == 37.5
        mxScalar.sum() == 1015.3125
        mxScalar.columnLabels == ["Diagonal"]
        mxScalar.rowLabels == ["Diagonal"]
    }

/*
    def "test covariance"() {
        given:

        when:
        // TODO implement stimulus
        thenCalculate:
        // TODO implement assertions
    }

    def "test correlation"() {
        given:

        when:
        // TODO implement stimulus
        thenCalculate:
        // TODO implement assertions
    }

    def "test toDataframe"() {
        given:

        when:
        // TODO implement stimulus
        thenCalculate:
        // TODO implement assertions
    }*/
}
