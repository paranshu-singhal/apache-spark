package edu.ucr.cs.paranshu;

import scala.Function;
import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.api.java.function.FlatMapFunction;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.SparkContext;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;

import java.util.Arrays;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

/**
 * WordCount
 */
public class WordCount {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("spark").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        countWords(args, sc);
        
    }

    static void countWords(String[] args, JavaSparkContext sc){

        final Accumulator<Integer> count = sc.accumulator(0);

        JavaRDD<String> rdd = sc.textFile(args[0]);

        JavaPairRDD<String, Double> pairRdd = rdd
        .flatMap(new FlatMapFunction<String,String>() {
            @Override
            public Iterator<String> call(String t) throws Exception {
                return Arrays.asList(t.split(" ")).iterator();
            }
        })
        .mapToPair(new PairFunction<String,String,Double>() {
            @Override
            public Tuple2<String,Double> call(String t) throws Exception {
                count.add(1);
                return new Tuple2<String,Double>(t.replaceAll("\\p{Punct}", " ").toLowerCase().trim(),(double)1);
            }
        })
        .reduceByKey(new Function2<Double,Double,Double>() {

            @Override
            public Double call(Double v1, Double v2) throws Exception {
                return (v1.doubleValue() + v2.doubleValue());
            }
        })
        .mapToPair(new PairFunction<Tuple2<String,Double>,Double,String>() {

            @Override
            public Tuple2<Double, String> call(Tuple2<String, Double> t) throws Exception {
                return t.swap();
            }
        })
        .sortByKey(false)
        .mapToPair(new PairFunction<Tuple2<Double,String>,String,Double>() {
            @Override
            public Tuple2<String, Double> call(Tuple2<Double, String> t) throws Exception {
                return t.swap();
            }
        });

        List<Tuple2<String, Double>> lineList = new ArrayList();
        for(Tuple2<String, Double> tuple: pairRdd.collect()){
            lineList.add(new Tuple2<String, Double> (tuple._1(), tuple._2()/(count.value().doubleValue())));
        }
        sc.parallelize(lineList).saveAsTextFile(args[1]); 

    }

}