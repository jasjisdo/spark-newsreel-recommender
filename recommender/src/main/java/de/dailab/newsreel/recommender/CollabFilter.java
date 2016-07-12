package de.dailab.newsreel.recommender;

import de.dailab.newsreel.recommender.common.item.Item;
import de.dailab.newsreel.recommender.common.recommender.Recommender;
import de.dailab.newsreel.recommender.common.util.SharedService;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import scala.Tuple2;
import org.apache.spark.api.java.function.Function;


/**
 * EXPERIMENTAL
 *
 * Created by jens on 08.12.15.
 */
@Deprecated
public class CollabFilter implements Recommender {

    private final String name = "Callaborative Filter Recommender";

    static final String LAMBDA = "lambda";
    static final String RANK = "rank";
    static final String ITERATIONS = "iterations";
    private int impressionCounter = 0;
    private HashMap<Long, Tuple<Model, JavaRDD>> modelTable = new HashMap<>();

    private JavaRDD<Rating> ratings;

    public CollabFilter() {
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void update(Item item, Long domainID) {
        impressionCounter++;
        JavaRDD<Item> rdd = SharedService.parallelize(item);
        // transform JavaRDD<Item> to JavaRDD<Rating>
        if (ratings == null)
            ratings = rdd.map(i -> new Rating(i.getItemID().intValue(), i.getUserID().intValue(), 1.));
        else
            ratings = ratings.union(rdd.map(i -> new Rating(i.getItemID().intValue(), i.getUserID().intValue(), 1.)));
        System.out.println(ratings.count());
        // add impression to user item matrix
        if (modelTable.containsKey(domainID)) {
            modelTable.put(domainID, new Tuple<>(modelTable.get(domainID).model,
                    modelTable.get(domainID).userItemMatrix.union(ratings)));
        }
        // if more than 9 new impressions came in, update the CF model and reset impression counter to 0
        if (impressionCounter > 9) {
            //impressionCounter = 0;
            //split the matrix into test set and train+validation set
            double[] splitWeights = {0.8, 0.2};
            JavaRDD<Rating>[] evalTestSplit = ratings.randomSplit(splitWeights);
            JavaRDD<Rating> trainAndValidationSet = evalTestSplit[0];
            JavaRDD<Rating> testSet = evalTestSplit[1];

            //Make parameter grid
            //Double[] lambdas = {0.1, 0.01, 0.001};
            //Integer[] ranks = {2, 4, 6, 8, 10};
            //Integer[] nIterations = {10, 15, 20};
            Double[] lambdas = {0.1};
            Integer[] ranks = {2};
            Integer[] nIterations = {5};
            HashMap paramGrid = new HashMap();
            paramGrid.put(LAMBDA, Arrays.asList(lambdas));
            paramGrid.put(RANK, Arrays.asList(ranks));
            paramGrid.put(ITERATIONS, Arrays.asList(nIterations));


            Model bestModel = CV(trainAndValidationSet, paramGrid, 3);
            //Final model evaluation, maybe not necessary in the updating procedure but only once for
            //the paper/report
            double performance = finalModelEval(bestModel.self, testSet);
            System.out.println(performance);
            modelTable.put(domainID, new Tuple<>(bestModel, ratings));
        }
    }

    @Override
    public List<Long> predict(Item item, Long domainID, Integer numberOfRequestedResults) {
        if (item == null) return null;
        if (numberOfRequestedResults == null) return null;

        //update(rdd,domainID);
        System.out.println("domainID "+domainID+"\nuserID "+item.getUserID());

        int itemID = item.getItemID().intValue();

        //Model and user item matrix are domain ID specific
        Tuple<Model, JavaRDD> modelMatrixTuple = modelTable.get(domainID);

        if (modelMatrixTuple == null) { // model is not trained yet
            // fallback solution
            return predictFallback(item,domainID,numberOfRequestedResults);
        }

        MatrixFactorizationModel model = modelMatrixTuple.model.self; // matrix is not null
        System.out.println("prodFeatures "+model.productFeatures().count());
        System.out.println("userFeatures "+model.userFeatures().count());

        Rating[] results = model.recommendProducts(itemID,numberOfRequestedResults);
        System.out.println("recommend: "+Arrays.toString(results));

        // Write only the item IDs into the result list
        List<Long> returnResult = new ArrayList<Long>();
        for (Rating r : results) {
            returnResult.add((long) r.product());
        }
        System.out.println(returnResult);
        return returnResult;
    }


    /**
     * Fallback prediction if the model is not trained yet.
     * @param item
     * @param domainID
     * @param numberOfRequestedResults
     * @return
     */
    private List<Long> predictFallback(Item item, Long domainID, int numberOfRequestedResults) {
        List<Long> returnResult = new ArrayList<Long>();
        for (int i=0; i < numberOfRequestedResults; i++){
            returnResult.add(new Long(item.getItemID() - i - 1));
        }
        return returnResult;
    }

    /**
     * The method CV does k-fold cross validation for Collaborative Filtering with ALS
     * input:
     * itemFeatureTuples: input RDD, consisting of ratings e.g.
     * !! Take care of splitting the data into test and training+validation before giving
     * training+validation as input to this method!
     * paramGrid: HashMap storing the different values for the model parameters (lambda, rank, nInterations)
     * nFolds: Number of folds the data should be splitted into
     * <p>
     * output:
     * bestModel: MatrixFactorizationModel with the smallest MSE -- best model according to cross validation
     * *
     */

    private Model CV(JavaRDD<Rating> itemFeatureTuples, HashMap<String, List> paramGrid, int nFolds) {
        List<Double> lambdas = paramGrid.get(LAMBDA);
        List<Integer> ranks = paramGrid.get(RANK);
        List<Integer> iterations = paramGrid.get(ITERATIONS);

        MatrixFactorizationModel bestModel = null;
        Double bestLambda = 0.0;
        Integer bestRank = 0;
        Integer bestIter = 0;
        double MSE = Double.MAX_VALUE;
        //Split the data into nFolds folds, assume that final test set has already been taken
        double[] weights = new double[nFolds];
        double weight = 1. / nFolds;
        for (int i = 0; i < nFolds; i++) {
            weights[i] = weight;
        }
        JavaRDD<Rating>[] folds = itemFeatureTuples.randomSplit(weights);
        //Start Cross validation for each parameter combination
        for (Double lambda : lambdas) {
            for (Integer rank : ranks) {
                for (Integer nIterations : iterations) {
                    System.out.println(lambda+", "+rank+", "+nIterations);
                    MatrixFactorizationModel tempModel = null;
                    double modelMSE = 0.;
                    for (int i = 0; i < nFolds; i++) {
                        // Choose validation set and join the remaining parts to form the training set
                        JavaRDD<Rating> validationSet = folds[i];
                        int alreadyTakenIndex;
                        JavaRDD<Rating> trainingSet;
                        if (i == 0) {
                            trainingSet = folds[i + 1];
                            alreadyTakenIndex = i + 1;
                        } else {
                            trainingSet = folds[i - 1];
                            alreadyTakenIndex = i - 1;
                        }
                        for (int j = 0; j < nFolds; j++) {
                            if (j != i && j != alreadyTakenIndex) trainingSet = trainingSet.union(folds[j]);
                        }
                        //Train the model
                        tempModel = ALS.train(JavaRDD.toRDD(trainingSet), rank, nIterations, lambda);

                        // Evaluate model on the validation fold
                        // remove ratings from validation set
                        JavaRDD<Tuple2<Object, Object>> validationData = validationSet.map(
                                new Function<Rating, Tuple2<Object, Object>>() {
                                    public Tuple2<Object, Object> call(Rating r) {
                                        return new Tuple2<Object, Object>(r.user(), r.product());
                                    }
                                }
                        );

                        // Let the model make predictions and store the results in a Tuple, user and item ID grouped
                        // in an extra tuple (<Tuple2<Integer, Integer>, Double>) instead of a Rating object
                        JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
                                tempModel.predict(JavaRDD.toRDD(validationData)).toJavaRDD().map(
                                        new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
                                            public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r) {
                                                return new Tuple2<Tuple2<Integer, Integer>, Double>(
                                                        new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating());
                                            }
                                        }
                                ));

                        // Join predictions with true ratings
                        JavaRDD<Tuple2<Double, Double>> ratesAndPreds =
                                JavaPairRDD.fromJavaRDD(validationSet.map(
                                        new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
                                            public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r) {
                                                return new Tuple2<Tuple2<Integer, Integer>, Double>(
                                                        new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating());
                                            }
                                        }
                                )).join(predictions).values();
                        // Calculate mean squared error
                        double foldMSE = JavaDoubleRDD.fromRDD(ratesAndPreds.map(
                                new Function<Tuple2<Double, Double>, Object>() {
                                    public Object call(Tuple2<Double, Double> pair) {
                                        Double err = pair._1() - pair._2();
                                        return err * err;
                                    }
                                }
                        ).rdd()).mean();

                        modelMSE += foldMSE;
                    }
                    modelMSE = modelMSE / nFolds;
                    if (modelMSE < MSE) {
                        bestModel = tempModel;
                        bestLambda = lambda;
                        bestRank = rank;
                        bestIter = nIterations;
                    }
                }
            }
        }
        // train again on the complete data set
        long start = System.currentTimeMillis();
        bestModel = ALS.train(ratings.rdd(),bestRank,bestIter, bestLambda);
        long end = System.currentTimeMillis();
        System.out.println("time for training: "+(end-start)/1000.0);
        return new Model(bestModel, bestLambda,bestIter,bestRank);
    }


    protected double finalModelEval(MatrixFactorizationModel model, JavaRDD<Rating> testSet) {
        // Evaluate model on the validation fold
        // remove ratings from validation set
        JavaRDD<Tuple2<Object, Object>> testData = testSet.map(
                new Function<Rating, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(Rating r) {
                        return new Tuple2<Object, Object>(r.user(), r.product());
                    }
                }
        );

        // Let the model make predictions and store the results in a Tuple, user and item ID grouped
        // in an extra tuple (<Tuple2<Integer, Integer>, Double>) instead of a Rating object
        JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
                model.predict(JavaRDD.toRDD(testData)).toJavaRDD().map(
                        new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
                            public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r) {
                                return new Tuple2<Tuple2<Integer, Integer>, Double>(
                                        new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating());
                            }
                        }
                ));

        // Join predictions with true ratings
        JavaRDD<Tuple2<Double, Double>> ratesAndPreds =
                JavaPairRDD.fromJavaRDD(testSet.map(
                        new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
                            public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r) {
                                return new Tuple2<Tuple2<Integer, Integer>, Double>(
                                        new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating());
                            }
                        }
                )).join(predictions).values();
        // Calculate mean squared error
        double MSE = JavaDoubleRDD.fromRDD(ratesAndPreds.map(
                new Function<Tuple2<Double, Double>, Object>() {
                    public Object call(Tuple2<Double, Double> pair) {
                        Double err = pair._1() - pair._2();
                        return err * err;
                    }
                }
        ).rdd()).mean();

        return MSE;
    }

    protected class Tuple<MatrixFactorizationModel, JavaRDD> implements Serializable{
        public final MatrixFactorizationModel model;
        public final JavaRDD userItemMatrix;

        public Tuple(MatrixFactorizationModel model, JavaRDD userItemMatrix) {
            this.model = model;
            this.userItemMatrix = userItemMatrix;
        }
    }

    private class Model implements Serializable{
        public MatrixFactorizationModel self;
        public Double lambda;
        public Integer iter;
        public Integer rank;

        public Model(MatrixFactorizationModel self, Double lambda, Integer iter, Integer rank) {
            this.self = self;
            this.lambda = lambda;
            this.iter = iter;
            this.rank = rank;
        }
    }
}
