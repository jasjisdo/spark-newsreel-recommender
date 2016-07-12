package de.dailab.newsreel.recommender.metarecommender.util;

/**
 * Created by domann on 07.12.15.
 */
public class InputVectorUtils {

    public enum VectorType {
        SIMPLE,
        LIST,
        CLUSTER
    }

    public enum InputVector {
        NULL(null),
        GENDER(VectorType.CLUSTER),
        AGE(VectorType.CLUSTER),
        INCOME(VectorType.CLUSTER),
        BROWSER(VectorType.SIMPLE);

        private final VectorType type;

        InputVector(VectorType type){
            this.type = type;
        }
    }

}
