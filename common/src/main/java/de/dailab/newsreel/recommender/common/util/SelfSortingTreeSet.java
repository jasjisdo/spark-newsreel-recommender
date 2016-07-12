package de.dailab.newsreel.recommender.common.util;

import de.dailab.newsreel.recommender.common.abstracts.ComparableObservable;

import java.io.Serializable;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Created by domann on 29.10.15.
 */
public class SelfSortingTreeSet<E extends ComparableObservable> implements NavigableSet<E>, Serializable, Observer {

    private TreeSet<E> set;

    private Supplier<TreeSet<E>> treeSetSupplier =
            (Supplier & Serializable)() -> new TreeSet<E>(Collections.reverseOrder());

    public SelfSortingTreeSet() {
        set = new TreeSet<>(Collections.reverseOrder());
    }

    public SelfSortingTreeSet(Collection c) {
        set = new TreeSet<>(Collections.reverseOrder());
        this.addAll(c);
    }

    public ArrayList<E> getCopyOfArrayList() {
        return new ArrayList<E>(set);
    }

    @Override
    public int size() {
        return set.size();
    }

    @Override
    public boolean isEmpty() {
        return set.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return set.contains(o);
    }

    @Override
    public E lower(E e) {
        return set.lower(e);
    }

    @Override
    public E floor(E e) {
        return set.floor(e);
    }

    @Override
    public E ceiling(E e) {
        return set.floor(e);
    }

    @Override
    public E higher(E e) {
        return set.higher(e);
    }

    @Override
    public E pollFirst() {
        E e = set.pollFirst();
        e.deleteObserver(this);
        return e;
    }

    @Override
    public E pollLast() {
        E e = set.pollLast();
        e.deleteObserver(this);
        return e;
    }

    @Override
    public Iterator iterator() {
        return set.iterator();
    }

    @Override
    public NavigableSet<E> descendingSet() {
        return set.descendingSet();
    }

    @Override
    public Iterator<E> descendingIterator() {
        return set.descendingIterator();
    }

    @Override
    public NavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
        return set.subSet(fromElement, fromInclusive, toElement, toInclusive);
    }

    @Override
    public NavigableSet<E> headSet(E toElement, boolean inclusive) {
        return set.headSet(toElement, inclusive);
    }

    @Override
    public NavigableSet<E> tailSet(E fromElement, boolean inclusive) {
        return set.tailSet(fromElement, inclusive);
    }

    @Override
    public Object[] toArray() {
        return set.toArray();
    }

    @Override
    public boolean add(E o) {
        o.addObserver(this);
        return set.add(o);
    }

    @Override
    public boolean remove(Object o) {
        ((E) o).deleteObserver(this);
        return set.remove(o);
    }

    @Override
    public boolean addAll(Collection c) {
        c.stream().forEach(e -> ((E) e).addObserver(this));
        return set.addAll(c);
    }

    @Override
    public void clear() {
        set.stream().forEach(e -> e.deleteObserver(this));
        set.clear();
    }

    @Override
    public boolean retainAll(Collection c) {
        set.stream()
            .filter(e -> !c.contains(e))
            .forEach(e2 -> e2.deleteObserver(this));
        return set.retainAll(c);
    }

    @Override
    public boolean removeAll(Collection c) {
        c.stream().forEach(e -> ((E) e).deleteObserver(this));
        return set.removeAll(c);
    }

    @Override
    public boolean containsAll(Collection c) {
        return set.containsAll(c);
    }

    @Override
    public E[] toArray(Object[] a) {
        return set.toArray((E[]) a);
    }

    @Override
    public Comparator<? super E> comparator() {
        return set.comparator();
    }

    @Override
    public SortedSet<E> subSet(E fromElement, E toElement) {
        return set.subSet(fromElement, toElement);
    }

    @Override
    public SortedSet<E> headSet(E toElement) {
        return set.headSet(toElement);
    }

    @Override
    public SortedSet<E> tailSet(E fromElement) {
        return tailSet(fromElement);
    }

    @Override
    public E first() {
        return set.first();
    }

    @Override
    public E last() {
        return set.last();
    }

    @Override
    public void update(Observable o, Object arg) {

        // resort set descending (default is ascending)
        set = set.stream().sorted().collect(Collectors.toCollection(treeSetSupplier));

    }

}
