package com.soto;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 统计每个单词的全局出现次数
 */
public class UpdateStateByKeyWordCount {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("UpdateStateByKeyWordCount")
                .setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));


        //1.如果使用 updateStateByKey牌子,必须设置checkpoint目录,开启checkpoint机制
        //这样就把每个key对应的state除了在内存中有,也要checkpoint一份,以便在内存数据丢失时.可以从checkpoint恢复
        jssc.checkpoint("hdfs://sotowang-pc:9000/wordcount_checkpoint");

        //基础wordcount逻辑
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

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

        //如何统计每个单词的全局出现次数??
        JavaPairDStream<String, Integer> wordCounts = pairs.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {

            //Optional类似于Scala中的样例类,就是Option,它代表了一个值的存在,可存在也可不存在
            //这两个参数values和state,每次batch计算的时候,都会调用这个函数
            //第一个参数values,相当于batch中key的新的值,可能有多个,比如一个hello,可能有两个1,(hello,1) (hello,1 ) 那么传入的是(1,1)
            //第二个参数,就是指这个key之前的状态,state,其中泛型的类型是自己指定的
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                //定义一个全局单词计数
                Integer newValue = 0;

                //判断state是否存在,如果不存在,说明是一个key第一次出现
                //如果存在,说明key之前已经统计过次数了
                if (state.isPresent()) {
                    newValue = state.get();
                }

                //将新出现的值都累加到newValues上去,就是key目前的全局统计次数
                for (Integer value : values) {
                    newValue += value;
                }
                return Optional.of(newValue);
            }

        });

        wordCounts.print();





        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }

}
