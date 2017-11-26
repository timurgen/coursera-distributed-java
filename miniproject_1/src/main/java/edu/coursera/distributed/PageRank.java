package edu.coursera.distributed;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

/**
 * A wrapper class for the implementation of a single iteration of the iterative
 * PageRank algorithm.
 */
public final class PageRank {

    /**
     * Default constructor.
     */
    private PageRank() {
    }

    /**
     * TODO Given an RDD of websites and their ranks, compute new ranks for all
     * websites and return a new RDD containing the updated ranks.
     *
     * Recall from lectures that given a website B with many other websites
     * linking to it, the updated rank for B is the sum over all source websites
     * of the rank of the source website divided by the number of outbound links
     * from the source website. This new rank is damped by multiplying it by
     * 0.85 and adding that to 0.15. Put more simply:
     *
     * new_rank(B) = 0.15 + 0.85 * sum(rank(A) / out_count(A)) for all A linking
     * to B
     *
     * For this assignment, you are responsible for implementing this PageRank
     * algorithm using the Spark Java APIs.
     *
     * The reference solution of sparkPageRank uses the following Spark RDD
     * APIs. However, you are free to develop whatever solution makes the most
     * sense to you which also demonstrates speedup on multiple threads.
     *
     * 1) JavaPairRDD.join 2) JavaRDD.flatMapToPair 3) JavaPairRDD.reduceByKey
     * 4) JavaRDD.mapValues
     *
     * @param sites The connectivity of the website graph, keyed on unique
     * website IDs.
     * @param ranks The current ranks of each website, keyed on unique website
     * IDs.
     * @return The new ranks of the websites graph, using the PageRank algorithm
     * to update site ranks.
     */
    public static JavaPairRDD<Integer, Double> sparkPageRank(
            final JavaPairRDD<Integer, Website> sites,
            final JavaPairRDD<Integer, Double> ranks) {
        //sites.distinct().groupByKey().cache();
        
        JavaPairRDD<Integer, Double> mappedValues = sites.join(ranks).flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Tuple2<Website, Double>>, Integer, Double>() {
            @Override
            public Iterable<Tuple2<Integer, Double>> call(Tuple2<Integer, Tuple2<Website, Double>> keyValue) throws Exception {
                Tuple2<Website, Double> website = keyValue._2();
                Website siteEdges = website._1();
                Double currentRank = website._2();
                
                List<Tuple2<Integer, Double>> contribs = new ArrayList<>(40);
                Iterator<Integer> iterator = siteEdges.edgeIterator();
                while (iterator.hasNext()) {
                    final int target = iterator.next();
                    contribs.add(new Tuple2(target, currentRank / (double) siteEdges.getNEdges()));
                }
                return contribs;
            }
        });
        JavaPairRDD<Integer, Double> resultedValues = mappedValues.reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double arg0, Double arg1) throws Exception {
                return arg0 * arg1;
            }
        }).mapValues(new Function<Double, Double>() {
            @Override
            public Double call(Double y) throws Exception {
                return 0.15 + 0.85 * y;
            }
        });
        
        return resultedValues;
    }
}
