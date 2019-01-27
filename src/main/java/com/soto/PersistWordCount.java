package com.soto;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 基于持久化机制的wordcount程序
 */
public class PersistWordCount {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("PersistWordCount")
                .setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));


        jssc.checkpoint("hdfs://sotowang-pc:9000/wordcount_checkpoint");

        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        JavaPairDStream<String, Integer> wordCounts = pairs.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

            //Optional类似于Scala中的样例类,就是Option,它代表了一个值的存在,可存在也可不存在
            //这两个参数values和state,每次batch计算的时候,都会调用这个函数
            //第一个参数values,相当于batch中key的新的值,可能有多个,比如一个hello,可能有两个1,(hello,1) (hello,1 ) 那么传入的是(1,1)
            //第二个参数,就是指这个key之前的状态,state,其中泛型的类型是自己指定的
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                Integer newValue = 0;

                if (state.isPresent()) {
                    newValue = state.get();
                }

                for (Integer value : values) {
                    newValue += value;
                }
                return Optional.of(newValue);
            }

        });

        //每次得到所有单词有统计次数以后,将其写入mysql存储进行拷入化,以便于后序的J2EE应用程序进行显示
        wordCounts.foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Integer> wordCountsRDD) throws Exception {
                wordCountsRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> wordCounts) throws Exception {
                        //给每个partition获取一个连接
                        Connection conn = ConnectionPool.getConnection();

                        //遍历partition中的数据,使用1个连接插入数据库
                        Tuple2<String, Integer> wordCount = null;
                        while (wordCounts.hasNext()) {
                            wordCount = wordCounts.next();

                            String sql = "insert into wordcount(word,count) " +
                                    " values( '" + wordCount._1 + "'," + wordCount._2 + ") ";

                            Statement stmt = conn.createStatement();
                            stmt.executeUpdate(sql);
                        }

                        //用完后还回去
                        ConnectionPool.returnConnection(conn);
                    }
                });
                return null;
            }
        });





        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }

}
