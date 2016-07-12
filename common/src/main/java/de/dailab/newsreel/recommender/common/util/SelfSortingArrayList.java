package de.dailab.newsreel.recommender.common.util;

import de.dailab.newsreel.recommender.common.abstracts.ComparableObservable;
import java.io.Serializable;
import java.util.*;
import java.util.function.Consumer;

/**
 * Created by domann on 29.10.15.
 */
public class SelfSortingArrayList<E extends ComparableObservable> implements List<E>, Serializable, Observer {

    private ArrayList<E> list;

    public SelfSortingArrayList() {
        list = new ArrayList<>();
    }

    public SelfSortingArrayList(Collection c) {
        list = new ArrayList<>();
        this.addAll(c);
    }

    public ArrayList<E> getCopyOfArrayList() {
        return new ArrayList<E>(list);
    }

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public boolean isEmpty() {
        return list.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return list.contains(o);
    }

    @Override
    public Iterator iterator() {
        return listIterator();
    }

    @Override
    public Object[] toArray() {
        return list.toArray();
    }

    @Override
    public boolean add(E o) {
        o.addObserver(this);
        boolean result = list.add(o);
        Collections.sort(list, Collections.reverseOrder());
        return result;
    }

    @Override
    public boolean remove(Object o) {
        ((E) o).deleteObserver(this);
        boolean result = list.remove(o);
        Collections.sort(list, Collections.reverseOrder());
        return result;
    }

    @Override
    public boolean addAll(Collection c) {
        c.stream().forEach(e -> ((E) e).addObserver(this));
        return list.addAll(c);
    }

    @Override
    public boolean addAll(int index, Collection c) {
        c.stream().forEach(e -> ((E) e).addObserver(this));
        boolean result = list.addAll(index ,c);
        Collections.sort(list, Collections.reverseOrder());
        return result;
    }

    @Override
    public void clear() {
        list.stream().forEach(e -> e.deleteObserver(this));
        list.clear();
    }

    @Override
    public E get(int index) {
        return list.get(index);
    }

    @Override
    public E set(int index, E element) {
        E replacedElement = list.set(index, element);
        replacedElement.deleteObserver(this);
        element.addObserver(this);
        Collections.sort(list, Collections.reverseOrder());
        return replacedElement;
    }

    @Override
    public void add(int index, E element) {
        list.get(index).deleteObserver(this);
        element.addObserver(this);
        list.add(index, element);
        Collections.sort(list, Collections.reverseOrder());
    }

    @Override
    public E remove(int index) {
        E removed = list.remove(index);
        removed.deleteObserver(this);
        Collections.sort(list, Collections.reverseOrder());
        return removed;
    }

    @Override
    public int indexOf(Object o) {
        return list.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return list.lastIndexOf(o);
    }

    @Override
    public ListIterator listIterator() {
        return new ListItr(this);
    }

    @Override
    public ListIterator listIterator(int index) {
        return new ListItr(this, index);
    }

    @Override
    public List subList(int fromIndex, int toIndex) {
        return list.subList(fromIndex, toIndex);
    }

    @Override
    public boolean retainAll(Collection c) {
        list.stream()
            .filter(e -> !c.contains(e))
            .forEach(e2 -> e2.deleteObserver(this));
        boolean result = list.retainAll(c);
        Collections.sort(list, Collections.reverseOrder());
        return result;
    }

    @Override
    public boolean removeAll(Collection c) {
        c.stream().forEach(e -> ((E) e).deleteObserver(this));
        return list.removeAll(c);
    }

    @Override
    public boolean containsAll(Collection c) {
        return list.containsAll(c);
    }

    @Override
    public E[] toArray(Object[] a) {
        return list.toArray((E[]) a);
    }

    @Override
    public void update(Observable o, Object arg) {

        // resort list
        Collections.sort(list, Collections.reverseOrder());

    }

    private class ListItr implements ListIterator<E> {

        private ListIterator<E> baseIter;

        private final SelfSortingArrayList<E> mother;

        public ListItr(SelfSortingArrayList<E> mother) {
            this.mother = mother;
            this.baseIter = mother.list.listIterator();
        }

        public ListItr(SelfSortingArrayList<E> mother, int index) {
            this.mother = mother;
            this.baseIter = mother.list.listIterator(index);
        }

        @Override
        public boolean hasNext() {
            return baseIter.hasNext();
        }

        @Override
        public E next() {
            return baseIter.next();
        }

        @Override
        public boolean hasPrevious() {
            return baseIter.hasPrevious();
        }

        @Override
        public E previous() {
            return baseIter.previous();
        }

        @Override
        public int nextIndex() {
            return baseIter.nextIndex();
        }

        @Override
        public int previousIndex() {
            return baseIter.previousIndex();
        }

        @Override
        public void remove() {
            baseIter.remove();
        }

        @Override
        public void forEachRemaining(Consumer<? super E> action) {
            baseIter.forEachRemaining(action);
        }

        @Override
        public void set(E e) {
            baseIter.set(e);
        }

        @Override
        public void add(E e) {
            baseIter.add(e);
        }
    }
}
