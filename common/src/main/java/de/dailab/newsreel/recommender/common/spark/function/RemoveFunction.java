package de.dailab.newsreel.recommender.common.spark.function;

import org.apache.spark.api.java.function.Function;

public class RemoveFunction<T1> implements Function<T1, Boolean> {
        
        T1 objectToRemove;
        
        public RemoveFunction(T1 objectToRemove) {
            this.objectToRemove = objectToRemove;
        }

        @Override
        public Boolean call(T1 t1) throws Exception {
            return !t1.equals(objectToRemove);
        }
        
    }