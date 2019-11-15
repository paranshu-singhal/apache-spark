package edu.ucr.cs.psing069;

import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

public class App {
    public static void main( String[] args ) {

        SparkConf conf = new SparkConf().setAppName("spark").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> inputFile = sc.textFile(args[0]);

		//Task - 1

        JavaPairRDD<Integer, Double> pairRdd = inputFile.mapToPair(new PairFunction<String, Integer, Double>() {
        	@Override
        	public Tuple2<Integer, Double> call(String t) throws Exception {
        	    String[] data = t.split("\t", -1);
        	    return new Tuple2<Integer, Double>(Integer.parseInt(data[5]), Double.parseDouble(data[6]));
        	}
        });
        JavaPairRDD<Integer, Iterable<Double>> groupRdd = pairRdd.groupByKey();
        List<String> lineList = new ArrayList<String>();

        for (Tuple2<Integer, Iterable<Double>> tuple : groupRdd.collect()) {
        	Iterator<Double> it = tuple._2.iterator();
        	double sum = 0.0;
        	int count = 0;
        	while (it.hasNext()) {
                sum += it.next();
                count++;
        	}
            lineList.add("Code "+ tuple._1 + ", average number of bytes = " + sum/count);
            
        }
        
        JavaRDD<String> lines = sc.parallelize(lineList);
        lines.saveAsTextFile( "task1.txt" );

        //Task - 2

        JavaPairRDD<Tuple2<String, String>, String> mappedLines = inputFile.mapToPair((PairFunction<String, Tuple2<String, String>, String>) s -> {
		    String[] parts = s.split("\t");
		    return new Tuple2<>(new Tuple2<>(parts[0], parts[4]), s);

		});

		JavaPairRDD<Tuple2<String, String>, Iterable<String>> groupedLines = mappedLines.groupByKey();

		JavaRDD<Tuple2<String, String>> joinResults = groupedLines
            .flatMap(new FlatMapFunction<Tuple2<Tuple2<String, String>, Iterable<String>>, Tuple2<String, String>>() {
                
                @Override public Iterator<Tuple2<String, String>> call(Tuple2<Tuple2<String, String>, Iterable<String>> input) throws Exception {

		                    ArrayList<String> lines = new ArrayList<String>();
		                    for (String line : input._2()){
                                lines.add(line);
                            }
		                    ArrayList<Tuple2<String, String>> answer = new ArrayList<>();
		                    for (int i = 0; i < lines.size(); i++) {
		                        int ti = Integer.parseInt(lines.get(i).split("\t")[2]);
		                        for (int j = i + 1; j < lines.size(); j++) {
		                            int tj = Integer.parseInt(lines.get(j).split("\t")[2]);
		                            if (Math.abs(ti - tj) < 60 * 60 * 1000) {
		                                answer.add(new Tuple2(lines.get(i), lines.get(j)));
		                            }
		                        }
		                    }
		                    return answer.iterator();
		                }
		            });
		    joinResults.saveAsTextFile("task2.txt");
    }
}
