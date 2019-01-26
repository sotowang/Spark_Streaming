package com.soto;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class KafkaReceiverWordCount {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("KafkaReceiverWordCount")
                .setMaster("local[2]");


        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        Map<String, Integer> topicThreadMap = new HashMap<String, Integer>();
        topicThreadMap.put("wordCount", 1);

        //使用KafkaUtils.createStream()方法创建针对Kafka的输入数据源
        //Kafka中返回的是JavaPair形式的,但每一个String为null,一般使用每二个参数
        JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(
                jssc,
                "192.168.12.218:2181,192.168.12.22:2181",
                "DefaultConsumerGroup",
                topicThreadMap
        );

        //开发WordCount逻辑
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public Iterable<String> call(Tuple2<String, String> tuple) throws Exception {
                return Arrays.asList(tuple._2.split(" "));
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
