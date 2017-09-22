package kic.interfaces;

/**
 * Created by kindler on 16/09/2017.
 */
public interface ToDouble<V> {
    ToDouble identity = (d -> d != null ? (Double) d : 0d);

    double toDouble(V value);
}
