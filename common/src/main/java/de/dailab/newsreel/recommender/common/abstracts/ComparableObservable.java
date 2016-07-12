package de.dailab.newsreel.recommender.common.abstracts;

import java.util.Observable;

/**
 * Created by domann on 22.12.15.
 */
public abstract class ComparableObservable<T> extends Observable implements Comparable<T> {

    @Override
    public abstract int compareTo(T t);

}
