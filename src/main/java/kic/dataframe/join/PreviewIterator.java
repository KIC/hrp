package kic.dataframe.join;

import java.util.Iterator;
import java.util.Optional;

/**
 * Created by kindler on 25/09/2017.
 */
public class PreviewIterator<E> implements Iterator<E> {
    private final Iterator<E> iterator;
    private Optional<E> preview;

    public PreviewIterator(Iterator<E> iterator) {
        this.iterator = iterator;
        this.preview = Optional.ofNullable(iterator.hasNext() ? iterator.next() : null);
    }

    public Optional<E> getPreview() {
        return preview;
    }

    @Override
    public boolean hasNext() {
        return preview.isPresent();
    }

    @Override
    public E next() {
        E value = preview.get();
        preview = Optional.ofNullable(iterator.hasNext() ? iterator.next() : null);
        return value;
    }
}
