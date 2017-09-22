package kic.dataframe;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;

/**
 * Created by kindler on 15/09/2017.
 */
public class Reshape<RK, CK, V> {
    private static final String STRING_DELIMITER = ", ";
    private static final String LINE_DELIMITER = "\n";
    private static final String NULL = "null";
    private static final String ROW = "ROW";
    private final DataFrame<RK, CK, V> dataFrame;

    public Reshape(DataFrame<RK, CK, V> dataFrame) {
        this.dataFrame = dataFrame;
    }

    public LinkedHashMap<RK, LinkedHashMap<CK, V>> toMapOfRowMaps() {
        return dataFrame.visit.walkRowWiseWithRowKey(
                new LinkedHashMap<>(),
                (rk, map) -> map.put(rk, new LinkedHashMap<>()),
                (rk, ck, v, map) -> map.get(rk).put(ck, v)
        );
    }

    public LinkedHashMap<CK, LinkedHashMap<RK, V>> toMapOfColumMaps() {
        return dataFrame.visit.walkColumnWiseWithColumnKey(
                new LinkedHashMap<>(),
                (ck, map) -> map.put(ck, new LinkedHashMap<>()),
                (ck, rk, v, map) -> map.get(ck).put(rk, v)
        );
    }

    @Override
    public String toString() {
        return dataFrame.visit.walkRowWise(
                new StringBuilder(ROW).append(STRING_DELIMITER).append(mkString(dataFrame.getColumnOrder())).append(STRING_DELIMITER),
                (rk, sb) -> sb.append(LINE_DELIMITER).append(rk).append(STRING_DELIMITER),
                (ck, v, sb) -> sb.append(v).append(STRING_DELIMITER)
        ).toString();
    }

    private String mkString(Collection<?> c) {
        return c.stream()
                .map(o -> o != null ? o.toString() : NULL)
                .collect(Collectors.joining(STRING_DELIMITER));
    }

}
