package kic.utils;

import kic.interfaces.Consumer3;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

public class MapUtil {
    public static <K, V>void exec(Map<K, V> map, K key, Consumer<V> action) {
        exec(map, key, false, action);
    }

    public static <K, V>void exec(Map<K, V> map, K key, boolean includeNull, Consumer<V> action) {
        V v = map.get(key);
        if (!includeNull && v != null) {
            action.accept(v);
        }
    }

    public static <K,V>Map<K,V> getAllEntries(Map<K,V> map, Collection<K> keys) {
        Map<K, V> result = new LinkedHashMap<>();
        for (K k : keys) {
            V v = map.get(k);
            if (v != null) result.put(k, v);
        }

        return result;
    }
}
