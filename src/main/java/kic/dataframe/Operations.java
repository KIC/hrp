package kic.dataframe;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public class Operations {

    public static <RK, CK, I, O>BiConsumer<DataFrame<RK, CK, I>, DataFrame<RK, CK, O>> deltaOperation(BiFunction<I, I, O> deltaCalculation) {
         return (window, result) -> {
            DataFrame.Tuple<RK, LinkedHashMap<CK, I>> buy = window.getFirstRow();
            DataFrame.Tuple<RK, LinkedHashMap<CK, I>> sell = window.getLastRow();
            Set<CK> intersect = new LinkedHashSet<>(buy.value.keySet());
            intersect.retainAll(sell.value.keySet());
            for (CK ck : intersect) {
                try {
                    O res = deltaCalculation.apply(sell.value.get(ck), buy.value.get(ck));
                    if (res != null) result.upsert(sell.rowKey, ck, res);
                } catch (NullPointerException npe) {
                    if (!(sell.value.get(ck) == null || buy.value.get(ck) == null)) {
                        throw npe;
                    } else {
                        // ignore the fact that one value was null (this also prevents us from storing null results)
                    }
                }
            }
        };
    }

}
