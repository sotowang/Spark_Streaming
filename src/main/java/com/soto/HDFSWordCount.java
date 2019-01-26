package com.soto;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class HDFSWordCount {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("HDFSWordCount")
                .setMaster("local[2]");


        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        //使用JavaStreamingContext的fileStream() 方法,创建针对HDFS目录的数据流
        JavaDStream<String> lines = jssc.textFileStream("hdfs://sotowang-pc:9000/wordcount_dir");

        //执行wordCoutn操作
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });

        //每个单词映射为(word,1)
        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });


        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
