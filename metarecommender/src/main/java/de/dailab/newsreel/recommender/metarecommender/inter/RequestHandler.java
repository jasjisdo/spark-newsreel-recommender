package de.dailab.newsreel.recommender.metarecommender.inter;

import java.io.Serializable;

/**
 * Created by domann on 18.12.15.
 */
public interface RequestHandler extends Serializable {

    String handleRequest(String... params) throws IllegalArgumentException;

}
