package com.soto;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;

/**
 * 基于滑动窗口的热点搜索词滑动统计
 */
public class WindowHotWord {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("TransformBlacklist")
                .setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));


        //日志格式:
        // tom world
        // leo hello
        JavaReceiverInputDStream<String> searcLog = jssc.socketTextStream("localhost", 9999);

        //将日志转换成只有一个的搜索词DS
        final JavaDStream<String> searchDStream = searcLog.map(new Function<String, String>() {
            @Override
            public String call(String log) throws Exception {
                return log.split(" ")[1];
            }
        });

        //映射为(word,1)
        JavaPairDStream<String, Integer> searchWordPairDStream = searchDStream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String log) throws Exception {
                return new Tuple2<>(log, 1);
            }
        });

        //针对(word,1)执行 reduceByKeyAndWindow操作,第二个参数是窗口长度,这里为60s,第三个为窗口间隔,这里为10s
        //每隔10s,将最近60s的数据,作为一个窗口,进行内部RDD聚合,然后统一对一个RDD进行后续计算
        JavaPairDStream<String, Integer> searchWordCountDStream = searchWordPairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, Durations.seconds(60),Durations.seconds(10));


        JavaPairDStream<String, Integer> finalDStream = searchWordCountDStream.transformToPair(new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
            @Override
            public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> searchWordCountsRDD) throws Exception {
                //执行搜索词与频率的反转
                JavaPairRDD<Integer, String> countSearchWordRDD = searchWordCountsRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
                        return new Tuple2<>(tuple._2, tuple._1);
                    }
                });

                //降序排序
                JavaPairRDD<Integer, String> sortedCountSearchWordRDD = countSearchWordRDD.sortByKey(false);

                //再次反转,变成(searchWord,count)
                JavaPairRDD<String, Integer> sortedSearchWordCountRDD = sortedCountSearchWordRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple) throws Exception {
                        return new Tuple2<>(tuple._2, tuple._1);
                    }
                });

                //用take()获取前3名搜索词
                List<Tuple2<String, Integer>> hotSearchWordsCount = sortedSearchWordCountRDD.take(3);

                for (Tuple2<String, Integer> searchWordCount : hotSearchWordsCount) {
                    System.out.println(searchWordCount);
                }

                return searchWordCountsRDD;
            }
        });

        //无关紧要,关键触发 DStream的output操作
        finalDStream.print();





        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }


}
