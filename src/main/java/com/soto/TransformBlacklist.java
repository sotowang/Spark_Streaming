package com.soto;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * 基于transform的实时黑名单过滤
 */
public class TransformBlacklist {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("UpdateStateByKeyWordCount")
                .setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        //模拟黑名单RDD
        List<Tuple2<String, Boolean>> blacklistData = new ArrayList<Tuple2 < String, Boolean >>();
        blacklistData.add(new Tuple2<String, Boolean>("tom", true));
        final JavaPairRDD<String, Boolean> blacklistRDD = jssc.sc().parallelizePairs(blacklistData);


        //日志格式,date username的方式
        JavaReceiverInputDStream<String> adsClickLog = jssc.socketTextStream("localhost", 9999);

        //先对输入的数据进行一下转换操作,变成(username,date username)
        //以便对后面每个batch RDD与定义好的黑名单进行join操作
        final JavaPairDStream<String, String> userAdsClickLogDStream = adsClickLog.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String adsClickLog) throws Exception {
                return new Tuple2<>(adsClickLog.split(" ")[1], adsClickLog);
            }
        });

        //将每个batch的RDD与黑名单RDD进行join操作,实时过滤
        JavaDStream<String> vaildAdsClickLogDStream = userAdsClickLogDStream.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> userAdsClickLogRDD) throws Exception {
                //左外连接,user不在黑名单中会被保存下来
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinedRDD = userAdsClickLogRDD.leftOuterJoin(blacklistRDD);
                //连接之后使用filter算子
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filteredRDD = joinedRDD.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                        if (tuple._2._2.isPresent() && tuple._2._2.get()) {
                            return false;
                        }
                        return true;
                    }
                });

                //此时filteredRDD为没有被黑名单用户点击的RDD,进行map操作转换为我们想要的格式
                JavaRDD<String> validAdsClickLogRDD = filteredRDD.map(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {
                    @Override
                    public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                        return tuple._2._1;
                    }
                });
                return validAdsClickLogRDD;
            }
        });

        vaildAdsClickLogDStream.print();






        jssc.start();
        jssc.awaitTermination();
        jssc.close();

    }
}
