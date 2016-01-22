package com.spark;

/**
 * Hello world!
 *
 */
 import java.util.Arrays;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


public class App
{
    public static void main( String[] args )
    {
      JavaSparkContext sc = new JavaSparkContext("local[2]", "Simple App");
      //read a file
      long startTime = System.currentTimeMillis();
      
      JavaRDD<String> textFile = sc.textFile("hdfs://localhost:8020/user/hdfs/test");
      // split all words in file
      JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
          public Iterable<String> call(String s) {
              return Arrays.asList(s.split(" "));
          }
      });
      JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
          public Tuple2<String, Integer> call(String s) {
              return new Tuple2<String, Integer>(s, 1);
          }
      });
      JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
          public Integer call(Integer a, Integer b) {
              return a + b;
          }
      });

      counts.saveAsTextFile("rst.txt");
      long endTime = System.currentTimeMillis();
      System.err.println("run time = : "+(endTime-startTime));
    }
}
