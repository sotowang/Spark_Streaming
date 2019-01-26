# Spark Streaming 基础

[北风网 Spark 2.0从入门到精通 (278讲)](https://www.bilibili.com/video/av19995678/?p=101&t=612
)
--- 

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

---
## DStream 和 Receiver

注:

```markdown
1. 使用本地模式,运行程序时,不能使用local或local[1],这样只会给执行输入DStream的executor分配一个线程,Spark Streaming至少需要2个线程,一条用来分配给Receiver接收数据,一条用来处理收到的数据
因此,必须使用local[n],n>=2

```

## 输入DStream之基础数据源以及基于HDFS的实时wordCount程序

### 基于socket,见上面例子

### HDFS文件  HDFSWordCount.java

其实就是监控HDFS目录,只要其中有新文件出现,就实时处理,相当时处理实时的文件流.

注:

>基于HDFS的数据源是没有Receiver的,不会占用cpu core

```java
 //使用JavaStreamingContext的fileStream() 方法,创建针对HDFS目录的数据流
JavaDStream<String> lines = jssc.textFileStream("hdfs://sotowang-pc:9000/wordcount_dir");
```

* 在HDFS创建wordcount_dir

``` bash
hadoop fs -mkdir /wordcount_dir
```

* 将文本传入hdfs

> hadoop fs -put /home/sotowang/user/aur/ide/idea/idea-IU-182.3684.101/workspace/Spark_Streaming/src/resources/hdfswordcount.txt /wordcount_dir/hdfswordcount.txt

结果

```markdown
Time: 1548490585000 ms
(hello,3)
(me,1)
(world,1)
(you,1)
```

### 基于Receiver方式实现Kafka数据源 KafkaReceiverWordCount.java


> 这种方式使用Receiver来获取数据。Receiver是使用Kafka的高层次Consumer API来实现的.
receiver从Kafka中获取的数据都是存储在Spark Executor的内存中的，然后Spark Streaming启动的job会去处理那些数据.
然而，在默认的配置下，这种方式可能会因为底层的失败而丢失数据.
如果要启用高可靠机制，让数据零丢失，就必须启用Spark Streaming的预写日志机制（Write Ahead Log，WAL）.
该机制会同步地将接收到的Kafka数据写入分布式文件系统（比如HDFS）上的预写日志中.所以，即使底层节点出现了失败，也可以使用预写日志中的数据进行恢复.
  

注:
```markdown
在默认配置下,这种方式可能因底层的失败而丢失数据,
如果启用高可靠机制,让数据零丢失,就必须启用Spark Streaming的预定日志机制(WAL),
该机制会同步地将接收到的Kafka数据写入分布式文件系统(如HDFS)上的预写日志中,所以,即使底层节点出现了失败,也可以使用预写日志中的数据进行恢复

```

* 添加依赖

```java
<!-- https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka -->
<dependency>
  <groupId>org.apache.spark</groupId>
  <artifactId>spark-streaming-kafka_2.10</artifactId>
  <version>1.6.0-cdh5.7.0</version>
</dependency>
```

* 查找本机内网ip

```bash
ip address
```

```java
Map<String, Integer> topicThreadMap = new HashMap<String, Integer>();
topicThreadMap.put("WordCount", 1);

//使用KafkaUtils.createStream()方法创建针对Kafka的输入数据源
//Kafka中返回的是JavaPair形式的,但每一个String为null,一般使用每二个参数
JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(
        jssc,
        "192.168.12.218:2181,192.168.12.22:21811",
        "DefaultConsumerGroup",
        topicThreadMap
);
```

* 启动Zookeeper

```markdown
zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties
```

* 启动kafka

```markdown
kafka-server-start.sh  -daemon $KAFKA_HOME/config/server.properties
```

* 创建topic

```markdown
kafka-console-producer.sh --broker-list localhost:9092 --wordCount
```

* 启动程序...

### 基于Direct方式实现Kafka数据源  KafkaDirectWordCount.java


>这种新的不基于Receiver的直接方式，是在Spark 1.3中引入的，从而能够确保更加健壮的机制。
替代掉使用Receiver来接收数据后，这种方式会周期性地查询Kafka，来获得每个topic+partition的最新的offset，从而定义每个batch的offset的范围。
当处理数据的job启动时，就会使用Kafka的简单consumer api来获取Kafka指定offset范围的数据。


这种方式有如下优点：

```markdown
1、简化并行读取：如果要读取多个partition，不需要创建多个输入DStream然后对它们进行union操作。
Spark会创建跟Kafka partition一样多的RDD partition，并且会并行从Kafka中读取数据。
所以在Kafka partition和RDD partition之间，有一个一对一的映射关系。

2、高性能：如果要保证零数据丢失，在基于receiver的方式中，需要开启WAL机制。
这种方式其实效率低下，因为数据实际上被复制了两份，Kafka自己本身就有高可靠的机制，会对数据复制一份，而这里又会复制一份到WAL中。
而基于direct的方式，不依赖Receiver，不需要开启WAL机制，只要Kafka中作了数据的复制，那么就可以通过Kafka的副本进行恢复。

3、一次且仅一次的事务机制：
基于receiver的方式，是使用Kafka的高阶API来在ZooKeeper中保存消费过的offset的。
这是消费Kafka数据的传统方式。
这种方式配合着WAL机制可以保证数据零丢失的高可靠性，但是却无法保证数据被处理一次且仅一次，可能会处理两次。
因为Spark和ZooKeeper之间可能是不同步的。
基于direct的方式，使用kafka的简单api，Spark Streaming自己就负责追踪消费的offset，并保存在checkpoint中。
Spark自己一定是同步的，因此可以保证数据是消费一次且仅消费一次。

```

```java
 //首先要创建一份kafka参数map
Map<String, String> kafkaParams = new HashMap<String, String>();
kafkaParams.put("metadata.broker.list", "sotowang-pc:9092");

//创建一个set,放置需要读取的topic
Set<String> topics = new HashSet<>();
topics.add("wordCount");

//创建输入DStream
JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(jssc,
        String.class,
        String.class,
        StringDecoder.class,
        StringDecoder.class,
        kafkaParams,
        topics);
```

---




































