package edu.ucr.cs.paranshu;

import scala.Function;
import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.FlatMapFunction;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;


/**
 * WordCount
 */
public class WordCount {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("spark").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile(args[0]);

        JavaPairRDD<String, Integer> pairRdd = rdd
        .flatMap(new FlatMapFunction<String,String>() {
            @Override
            public Iterator<String> call(String t) throws Exception {
                return Arrays.asList(t.split(" ")).iterator();
            }
        })
        .mapToPair(new PairFunction<String,String,Integer>() {
            @Override
            public Tuple2<String,Integer> call(String t) throws Exception {
                return new Tuple2<String,Integer>(t.replaceAll("\\p{Punct}", " ").toLowerCase().trim(),1);
            }
        })
        .reduceByKey(new Function2<Integer,Integer,Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        })
        .mapToPair(new PairFunction<Tuple2<String,Integer>,Integer,String>() {

            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                return t.swap();
            }
        })
        .sortByKey(false)
        .mapToPair(new PairFunction<Tuple2<Integer,String>,String,Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                return t.swap();
            }
        });
        pairRdd.saveAsTextFile(args[1]);
    }

}