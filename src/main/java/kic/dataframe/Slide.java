package kic.dataframe;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Created by kindler on 25/09/2017.
 */
public class Slide<RK, CK, V> {
    private final DataFrame<RK,CK, V> dataFrame;

    public Slide(DataFrame<RK, CK, V> dataFrame) {
        this.dataFrame = dataFrame;
    }


    public <CK2,V2>DataFrame<RK, CK2, V2> rows(Function<DataFrame<RK,CK,V>, Map<CK2, V2>> windowFunction) {
        return window(1, windowFunction);
    }

    public <CK2,V2>DataFrame<RK, CK2, V2> window(int windowSize, Function<DataFrame<RK,CK,V>, Map<CK2, V2>> windowFunction) {
        final DataFrame<RK,CK2,V2> result = new DataFrame<>();
        slide(result, windowSize, (win, res) -> windowFunction.andThen(r -> r != null ? r : new HashMap<CK2, V2>())
                                                              .apply(win).forEach((k, v) -> res.upsert(win.lastRowKey(), k, v)));
        return result;
    }

    @Deprecated
    public <RK2,CK2,V2>DataFrame<RK2, CK2, V2> slide(int windowSize, BiConsumer<DataFrame<RK,CK,V>, DataFrame<RK2,CK2,V2>> windowFunction) {
        DataFrame<RK2,CK2,V2> result = new DataFrame<>();
        slide(result, windowSize, windowFunction);
        return result;
    }

    private <RK2, CK2, V2>void slide(DataFrame<RK2,CK2,V2> result, int windowSize, BiConsumer<DataFrame<RK,CK,V>, DataFrame<RK2,CK2,V2>> windowFunction) {
        List<RK> rowKeys = new LinkedList<>();
        for (RK rk : dataFrame.getRowOrder()) {
            rowKeys.add(rk);
            while (rowKeys.size() > windowSize) {
                Iterator<RK> it = rowKeys.iterator();
                it.next();
                it.remove();
            }

            if (rowKeys.size() >= windowSize) {
                DataFrame<RK, CK, V> window = new DataFrame<>(dataFrame, rowKeys, dataFrame.getColumnOrder());
                windowFunction.accept(window, result);
            }
        }
    }
}
