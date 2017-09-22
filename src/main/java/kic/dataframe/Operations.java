package kic.dataframe;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public class Operations {

    public static <RK, CK, I, O>BiConsumer<DataFrame<RK, CK, I>, DataFrame<RK, CK, O>> deltaOperation(BiFunction<I, I, O> deltaCalculation) {
        return (window, result) -> {
            for (CK ck : window.getColumnOrder()) {
                try {
                    I sell = window.getElement(window.lastRowKey(), ck);
                    I buy = window.getElement(window.firstRowKey(), ck);
                    if (buy != null && sell != null) {
                        O res = deltaCalculation.apply(sell, buy);
                        if (res != null) result.upsert(window.lastRowKey(), ck, res);
                    }
                } catch (NullPointerException npe) {
                    // ignore the fact that one value was null (this also prevents us from storing null results)
                }
            }
        };
    }

}
