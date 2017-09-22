package kic.dataframe;

import kic.interfaces.Consumer3;
import kic.interfaces.Consumer4;

import java.util.List;
import java.util.function.BiConsumer;

/**
 * Created by kindler on 15/09/2017.
 */
public class Visitor<RK, CK, V> {
    private final DataFrame<RK, CK, V> dataFrame;

    public Visitor(DataFrame<RK, CK, V> dataFrame) {
        this.dataFrame = dataFrame;
    }

    public <R>R walkRowWise(R value, BiConsumer<RK, R> rowWalker, Consumer3<CK, V, R> columnWalker) {
        return walkRowWiseWithRowKey(value, rowWalker, (rk, ck, v, r) -> columnWalker.accept(ck, v, r));
    }

    public <R>R walkRowWiseWithRowKey(R value, BiConsumer<RK, R> rowWalker, Consumer4<RK, CK, V, R> columnWalker) {
        List<CK> colKeys = dataFrame.getColumnOrder();
        dataFrame.getRowOrder().forEach(rk -> {
            rowWalker.accept(rk, value);
            colKeys.forEach(ck -> columnWalker.accept(rk, ck, dataFrame.getElement(rk, ck), value));
        });

        return value;
    }

    public <R>R walkColumnWiseWithColumnKey(R value, BiConsumer<CK, R> columnWalker, Consumer4<CK, RK, V, R> rowWalker) {
        List<RK> rowKeys = dataFrame.getRowOrder();
        dataFrame.getColumnOrder().forEach(ck -> {
            columnWalker.accept(ck, value);
            rowKeys.forEach(rk -> rowWalker.accept(ck, rk, dataFrame.getElement(rk, ck), value));
        });

        return value;
    }
}
