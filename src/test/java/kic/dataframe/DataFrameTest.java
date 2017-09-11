package kic.dataframe;

import org.junit.Test;

import java.util.function.BiConsumer;

/**
 * Created by kindler on 08/09/2017.
 */
public class DataFrameTest {

    @Test
    public void fooTest() throws Exception {
        DataFrame<String, String, Double> df = new DataFrame();
        df.upsert("1", "a", 1d);
        df.upsert("1", "b", 2d);
        df.upsert("2", "b", 3d);

        df.upsert("1", "a", 1d);
        df.upsert("1", "b", 2d);
        df.upsert("2", "b", 3d);


        System.out.println(df);

        df.upsert("2", "a", 4d);
        System.out.println(df);
        System.out.println(df.asDoubleMatrix(d -> d, true));

        df.upsert("3", "a", 5d);
        df.upsert("3", "b", 6d);
        System.out.println(df);

        DataFrame<String, String, String> lala = df.slide(2, (window, result) -> System.out.println(window));

        BiConsumer<DataFrame<String, String, Double>, DataFrame<String, String, Double>> calcReturn = Operations.deltaOperation((sell, buy) -> sell / buy - 1d);

        DataFrame<String, String, Double> returns = df.slide(2, calcReturn);
        System.out.println(returns);
    }
}
