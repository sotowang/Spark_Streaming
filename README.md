# Spark Streaming 基础

[北风网 Spark 2.0从入门到精通 (278讲)](https://www.bilibili.com/video/av19995678/?p=101&t=612
)

实时数据源头--> 消息中间件(Kafka)

SparkStreaming 是Spark Core的一种扩展

工作原理:

> 接收实时输入数据流,将数据拆分为多个batch,比如每收集1秒的数据封装为一个batch,然后将每个batch交给Spark的计算引擎进行处理,最后会形成一个结果数据流其中的数据也是由一个一个的batch组成的


```markdown
(Kafka,Flume,HDFS/S3,Kinesis,Twitter) ==> (Spark Streaming) ==> (HDFS, DataBase,Dashboards)
```

## Spark Streaming 与 Strom对比

###  Spark Streaming

* 准实时,秒级,吞吐量高,健壮性一般(checkpoint,WAL),不支持动态调整并行度,事务机制不够完善

* 最大的优势是位于Spark生态技术栈中

### Strom

* 纯实时(来一条数据,处理一条数据),毫秒级,吞吐量低,健壮性强(Zookeeper,ACK),支持动态调整并行度,支持事务机制完善

## Spark Streaming: 实时WordCount程序开发  WordCount.java

*  linux下netcat安装

> sudo apt-get install netcat

---

```java
public class WordCount {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("WordCount")
                .setMaster("local[2]");


        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        //首先,创建输入DStream,代表一个数据源(比如Kafka,socket)来的不断的实时数据

        //jssc.socketTextStream() -->数据源为socket网络端口的数据流
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

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
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        //必须调用JavaSreamingContext.start() 整个SparkStreaming才会执行,否则不会执行
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
```

* nc启动

> nc -l localhost -p 9999








































