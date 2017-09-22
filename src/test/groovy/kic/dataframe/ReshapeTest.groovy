package kic.dataframe

import spock.lang.Specification

/**
 * Created by kindler on 15/09/2017.
 */
class ReshapeTest extends Specification {

    def "represent a dataframe as a string"() {

        given:
        DataFrame df = new DataFrame<>()
        df.upsert(1, "a", 10)
        df.upsert(1, "b", 11)
        df.upsert(2, "a", 12)

        when:
        def s = df.toString()

        then:
        s == "ROW, a, b, \n1, 10, 11, \n2, 12, null, "

    }
}
