package de.dailab.newsreel.recommender.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by jens on 19.01.16.
 */

public class LimitedList<E> extends ArrayList<E> {

    private final int size;
    private int it;

    public LimitedList(int size) {
        this.size = size;
        this.it = 0;
    }

    public List<E> sorted() {
        ArrayList<E> list = new ArrayList<>();
        for (int i = (it + 1) % this.size(); i != it; i = (i + 1) % this.size()) {
            list.add(this.get(i));
        }
        return list;
    }

    @Override
    public boolean add(E e) {
        add(it, e);
        return true;
    }

    @Override
    public void add(int i, E e) {
        it = (i + 1) % size;
        if (super.size() < size) super.add(e);
        else super.set(it, e);
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        it = index % size;
        c.forEach(item -> add(it, item));
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        addAll(it, c);
        return true;
    }

    @Override
    public E set(int index, E element) {
        if (index >= size)
            throw new IndexOutOfBoundsException("Index: "+index+", Size: "+size);
        E tmp = get(index);
        add(index, element);
        return tmp;
    }
}
