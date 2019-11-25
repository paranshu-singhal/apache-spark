package edu.ucr.cs.paranshu;

import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

/**
 * App
 */
public class App {

     public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("spark").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        queryData(args, sc);
     }

     static void queryData(String[] args, JavaSparkContext sc){

        JavaRDD<Tuple2<String,String>> rdd = sc.textFile(args[0])
            .mapToPair(new PairFunction<String,Tuple2<String, String>,String>() {

                @Override
                public Tuple2<Tuple2<String, String>, String> call(String t) throws Exception {

                    String[] s = t.split("\t");

                    return new Tuple2<Tuple2<String,String>,String>(new Tuple2<String,String>(s[0], s[4]), t);
                }
                
            })
            .groupByKey()
            .flatMap( new FlatMapFunction<Tuple2<Tuple2<String,String>,Iterable<String>>, Tuple2<String, String>>() {

                @Override
                public Iterator<Tuple2<String, String>> call(Tuple2<Tuple2<String, String>, Iterable<String>> t) throws Exception {

                    List<String> lines = new ArrayList();

                    for(String line : t._2()){
                        lines.add(line);
                    }
                    List<Tuple2<String, String>> result = new ArrayList();

                    for(int i=0; i<lines.size()-1; i++){

                        int t1 = Integer.parseInt( lines.get(i).split("\t")[2]);
                        for(int j=i+1; j<lines.size(); j++){
                            int t2 = Integer.parseInt( lines.get(j).split("\t")[2]);
                            if(Math.abs(t1-t2) <= 3600 * 1000){
                                result.add(new Tuple2<String,String>(lines.get(i), lines.get(j)));
                            }
                        }
                    }

                    return result.iterator();
                }
            });

            rdd.saveAsTextFile(args[1]);
     }
}