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

注: zookeeper无法关闭问题

1. 查找该端口号2181 进程

```markdown
netstat -anp|grep 2181
```

2. 杀死进程

```markdown
kill -9 PID
```


---

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

## DStream transform操作

### updateStateByKey 统计每个单词的全局出现次数  UpdateStateByKeyWordCount.java

注: 必须开启checkpoint()

以DStream中的数据进行按key做reduce操作，然后对各个批次的数据进行累加 
在有新的数据信息进入或更新时，可以让用户保持想要的任何状。使用这个功能需要完成两步： 

```markdown
1) 定义状态：可以是任意数据类型 
2) 定义状态更新函数：用一个函数指定如何使用先前的状态，从输入流中的新值更新状态。 
```

对于有状态操作，要不断的把当前和历史的时间切片的RDD累加计算，随着时间的流失，计算的数据规模会变得越来越大。



```java
//1.如果使用 updateStateByKey牌子,必须设置checkpoint目录,开启checkpoint机制
//这样就把每个key对应的state除了在内存中有,也要checkpoint一份,以便在内存数据丢失时.可以从checkpoint恢复
jssc.checkpoint("hdfs://sotowang-pc:9000/wordcount_checkpoint");
```

```java
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
```

* 创建HDFS中wordcount_checkpoint文件

```markdown
hadoop fs -mkdir /wordcount_checkpoint
```

* socket流 

```markdown
nc -l localhost -p 9999
```

---

### 案例: 基于transform的实时黑名单过滤  TransformBlacklist.java

背景: 用户对我们网站上的广告可以进行占地,点击之后要进行时实的计费,点一次算一下钱,但对于某些帮助无良商家刷广告的人,那么我们有一个黑名单,只要黑名单中
的用户点击广告,就过滤掉

```java
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
```

* 打开socket流

```markdown
nc -l localhost -p 9999
```

---

## Spark Streaming 滑动窗口

案例:  热点搜索词滑动统计,每隔10s统计最近60秒钏的搜索词的搜索频次,并打印出排名最靠前的3个搜索词以及出现次数

### 基于滑动窗口的热点搜索词滑动统计  WindowHotWord.java

```java
//针对(word,1)执行 reduceByKeyAndWindow操作,第二个参数是窗口长度,这里为60s,第三个为窗口间隔,这里为10s
//每隔10s,将最近60s的数据,作为一个窗口,进行内部RDD聚合,然后统一对一个RDD进行后续计算
JavaPairDStream<String, Integer> searchWordCountDStream = searchWordPairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
    @Override
    public Integer call(Integer v1, Integer v2) throws Exception {
        return v1 + v2;
    }
}, Durations.seconds(60),Durations.seconds(10));
```



* 启动socket流

```markdown
nc -l localhost -p 9999
```

* 启动程序...


---
## Spark Streaming中的output操作

DStream中的所有计算,都是由output操作触发的,比如print(),如果没有任何的output操作,就不会执行定义的计算逻辑

此外,即使使用了foreachRDD output操作,也必须在里面对RDD执行action操作,才能触发对每一个batch的计算逻辑.否则,光有foreachRDD output操作,在里面没有对RDD
执行action操作,也不会触发任何逻辑.

### foreachRDD

通常在foreachRDD中都会创建一个Connection,比如JDBC Connection,然后通过Connection将数据写入外部存储

误区:

> 1. 在RDD的foreach操作外部,创建Connection

这种方式是错误的,因为它会导致Connection对象被序列化后传输到每个task中,而这种Connection对象,实际上是不支持序列化的,也就无法被传输

```scala
dstream.foreachRDD{rdd =>
    val connection = createNewConncetion()
    rdd.foreach{
        record => connection.send(record)
    }
}
```

> 2. 在RDD的foreach操作内部创建Connection

这种方式是可以的,但效率低下,因为它会导致对于RDD中的每一条数据都创建一个Connection对象,而通常来说,Connection的创建,很消耗性能

```scala
dstream.foreachRDD{rdd =>
    rdd.foreach{
        record => 
            val connection = createNewConncetion()
            connection.send(record)
            connection.close()
    }
}
```

合理的方式:

> 1. 使用RDD的foreachPartition操作,并且在该操作内部创建Connection对象,这样就相当于为RDD的每个Partition创建一个Connection对象,节省资源

```scala
dstream.foreachRDD{rdd =>
    rdd.foreachPartition{
        partitionOfRecords => 
            val connection = createNewConncetion()
            partitionOfRecords.foreach(record => connection.send(record))
            connection.close()
    }
}
```

> 2. 自己手动封装一个静态连接池,使用RDD的foreachPartition操作,并且在该操作内部,从静态连接池中,通过静态方法,获取到一个连接,使用之后再还回去.
这样的话,甚至在多个RDD的partition之间,也可以复用连接了,而且可以让连接池采取懒创建的策略,并且空闲一段时间后将其释放掉

```scala
dstream.foreachRDD{rdd =>
    rdd.foreachPartition{
        partitionOfRecords => 
            val connection = ConnectionPool.getConnection()
            partitionOfRecords.foreach(record => connection.send(record))
            ConnectionPool.returnConnection(Connection)
    }
}
```

#### foreachRDD 实战  PersistWordCount.java

案例: 改写UpdateStateByKeyWordCount,将每次统计出来的全局的单词计数,写到MySQL数据库中

* 建表

```mysql
create table wordcount(
  id integer auto_increment primary key,
  updated_time timestamp not null default current_timestamp on update current_timestamp,
  word varchar(255),
  count integer
);
```

[MySQL CURRENT_TIMESTAMP 和 ON UPDATE CURRENT_TIMESTAMP 详解](https://blog.csdn.net/chenshun123/article/details/79677433)

```markdown
1> CURRENT_TIMESTAMP : 当要向数据库执行 insert操作时，如果有个 timestamp字段属性设为 CURRENT_TIMESTAMP，则无论这个字段有没有set值都插入当前系统时间

2> ON UPDATE CURRENT_TIMESTAMP : 使用 ON UPDATE CURRENT_TIMESTAMP 放在 TIMESTAMP 类型的字段后面，在数据发生更新时该字段将自动更新时间
```

* 连接池 ConnectionPool.java

```java
/**
     * 获取连接,多线程访问并发控制
     * @return
     */
    public synchronized static Connection getConnection() {
        try{
            if (connectionQueue == null) {
                connectionQueue = new LinkedList<Connection>();
                for (int i = 0; i < 10; i++) {
                    Connection conn = DriverManager.getConnection("jdbc:mysql://sotowang-pc:3306/testdb",
                            "root",
                            "123456");
                    connectionQueue.push(conn);
                }
            }

        }catch (Exception e){
            e.printStackTrace();
        }

        //poll :移除并返问队列头部的元素    如果队列为空，则返回null
        return connectionQueue.poll();
    }
```

* 写入Mysql

```java
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
```

* 启动socket流

```markdown
nc -l localhost -p 9999
```

* 启动程序...

---

## Spark Streaming 与 Spark Core整合  Top3HotProduct.java

案例: 每隔10s统计最近60s的每个种类的每个商品的点击次数,然后统计出每个各类top3的热门商品

注: 

> 1. 开窗函数row_number() 如果使用SqlContext会报错,

解决方法: 使用HiveContext

---

## Spark Streaming缓存与持久化机制

与RDD类似，Spark Streaming也可以让开发人员手动控制，将数据流中的数据持久化到内存中。
对DStream调用persist()方法，就可以让Spark Streaming自动将该数据流中的所有产生的RDD，都持久化到内存中。
如果要对一个DStream多次执行操作，那么，对DStream持久化是非常有用的。因为多次操作，可以共享使用内存中的一份缓存数据。

对于基于窗口的操作，比如reduceByWindow、reduceByKeyAndWindow，以及基于状态的操作，比如updateStateByKey，默认就隐式开启了持久化机制。
即Spark Streaming默认就会将上述操作产生的Dstream中的数据，缓存到内存中，不需要开发人员手动调用persist()方法。

对于通过网络接收数据的输入流，比如socket、Kafka、Flume等，默认的持久化级别，是将数据复制一份，以便于容错。相当于是，用的是类似MEMORY_ONLY_SER_2。

与RDD不同的是，默认的持久化级别，统一都是要序列化的。


## Spark Streaming Checkpoint机制

[Spark Streaming缓存、Checkpoint机制](https://blog.csdn.net/u010521842/article/details/78074354)

Spark Streaming应用程序如果不手动停止，则将一直运行下去，
在实际中应用程序一般是24小时*7天不间断运行的，因此Streaming必须对诸如系统错误，JVM出错等与程序逻辑无关的错误(failures)具体很强的弹性，
具备一定的非应用程序出错的容错性。Spark Streaming的Checkpoint机制便是为此设计的，它将足够多的信息checkpoint到某些具备容错性的存储系统如hdfs上，
以便出错时能够迅速恢复。有两种数据可以checkpoint：

Metadata checkpointing

```markdown

将流式计算的信息保存到具备容错性的存储上比如HDFS，Metadata Checkpointing适用于当streaming应用程序Driver所在的节点出错时能够恢复，

元数据包括： 
Configuration(配置信息) : 创建streaming应用程序的配置信息 
Dstream operations : 在streaming应用程序中定义的DStreaming操作 
Incomplete batches : 在队列中没有处理完的作业
```

Data checkpointing

```markdown

将生成的RDD保存到外部可靠的存储当中，对于一些数据跨度为多个batch的有状态transformation操作来说，checkpoint非常有必要，
因为在这些transformation操作生成的RDD对前一RDD有依赖，随着时间的增加，依赖链可能非常长，checkpoint机制能够切断依赖链，将中间的RDD周期性地checkpoint到可靠存储当中，
从而在出错时可以直接从checkpoint点恢复。 
具体来说，metadata checkpointing主要还是从driver失败中恢复，而Data Checkpoint用于对有状态的transformation操作进行checkpointing
```

### 什么时候需要启用checkpoint？

什么时候该启用checkpoint？满足一下任意条件： 

```markdown

- 使用了stateful转换，如果application中使用了updateStateByKey或者reduceByKeyAndWindow等stateful操作，必须提供checkpoint目录来允许定时的RDD checkpoint 

- 希望能从意外中恢复driver
```

如果streaming app没有stateful操作，也允许Driver挂掉之后再次重启 的进度丢失，就没有启动checkpoint的必要了。

### 如何使用checkpoint？
启用checkpoint，需要设置一个支持容错的、可靠的文件系统(如hdfs、s3等)目录来保存checkpoint数据。
通过调用streamingContext.checkpoint(checkpointDirectory)来完成。另外 ，如果你 想让你的application能从driver失败中恢复，你的application要满足 : 

```markdown
- 若application为首次重启，将创建一个新的StreamContext实例 
- 如果application是从失败中重启，将会从checkpoint目录导入checkpoint数据来重新创建StreamingContext实例。

```

## Spark Streaming 部署、升级和监控应用程序

[Spark Streaming 部署、升级和监控应用程序](http://www.manongjc.com/article/47473.html)

### 部署应用程序

* 有一个集群资源管理器，比如standalone模式下的Spark集群，Yarn模式下的Yarn集群等。

* 打包应用程序为一个jar包，课程中一直都有演示。

* 为executor配置充足的内存，因为Receiver接受到的数据，是要存储在Executor的内存中的，所以Executor必须配置足够的内存来保存接受到的数据。
要注意的是，如果你要执行窗口长度为10分钟的窗口操作，那么Executor的内存资源就必须足够保存10分钟内的数据，因此内存的资源要求是取决于你执行的操作的。

* 配置checkpoint，如果你的应用程序要求checkpoint操作，那么就必须配置一个Hadoop兼容的文件系统（比如HDFS）的目录作为checkpoint目录.

* 配置driver的自动恢复，如果要让driver能够在失败时自动恢复，之前已经讲过，一方面，要重写driver程序，一方面，要在spark-submit中添加参数。


### 部署应用程序：启用预写日志机制
* 预写日志机制，简写为WAL，全称为Write Ahead Log。从Spark 1.2版本开始，就引入了基于容错的文件系统的WAL机制。
如果启用该机制，Receiver接收到的所有数据都会被写入配置的checkpoint目录中的预写日志。
这种机制可以让driver在恢复的时候，避免数据丢失，并且可以确保整个实时计算过程中，零数据丢失。

* 要配置该机制，首先要调用StreamingContext的checkpoint()方法设置一个checkpoint目录。然后需要将spark.streaming.receiver.writeAheadLog.enable参数设置为true。

* 然而，这种极强的可靠性机制，会导致Receiver的吞吐量大幅度下降，因为单位时间内，有相当一部分时间需要将数据写入预写日志。
如果又希望开启预写日志机制，确保数据零损失，又不希望影响系统的吞吐量，那么可以创建多个输入DStream，启动多个Rceiver。

* 此外，在启用了预写日志机制之后，推荐将复制持久化机制禁用掉，因为所有数据已经保存在容错的文件系统中了，不需要在用复制机制进行持久化，保存一份副本了。
只要将输入DStream的持久化机制设置一下即可，persist(StorageLevel.MEMORY_AND_DISK_SER)。（之前讲过，默认是基于复制的持久化策略，_2后缀）


### 部署应用程序：设置Receiver接收速度

* 如果集群资源有限，并没有大到，足以让应用程序一接收到数据就立即处理它，Receiver可以被设置一个最大接收限速，以每秒接收多少条单位来限速。

* spark.streaming.receiver.maxRate和spark.streaming.kafka.maxRatePerPartition参数可以用来设置，前者设置普通Receiver，后者是Kafka Direct方式。

* Spark 1.5中，对于Kafka Direct方式，引入了backpressure机制，从而不需要设置Receiver的限速，Spark可以自动估计Receiver最合理的接收速度，并根据情况动态调整。
只要将spark.streaming.backpressure.enabled设置为true即可。

* 在企业实际应用场景中，通常是推荐用Kafka Direct方式的，特别是现在随着Spark版本的提升，越来越完善这个Kafka Direct机制。

```markdown
优点：
1、不用receiver，不会独占集群的一个cpu core；
2、有backpressure自动调节接收速率的机制；
3、…。

```

### 升级应用程序

*由于Spark Streaming应用程序都是7 * 24小时运行的。因此如果需要对正在运行的应用程序，进行代码的升级，那么有两种方式可以实现：

```markdown
 1. 升级后的Spark应用程序直接启动，先与旧的Spark应用程序并行执行。当确保新的应用程序启动没问题之后，就可以将旧的应用程序给停掉。
 但是要注意的是，这种方式只适用于，能够允许多个客户端读取各自独立的数据，也就是读取相同的数据。

 2. 小心地关闭已经在运行的应用程序，使用StreamingContext的stop()方法，可以确保接收到的数据都处理完之后，才停止。
 然后将升级后的程序部署上去，启动。这样，就可以确保中间没有数据丢失和未处理。因为新的应用程序会从老的应用程序未消费到的地方，继续消费。
 但是注意，这种方式必须是支持数据缓存的数据源才可以，比如Kafka、Flume等。如果数据源不支持数据缓存，那么会导致数据丢失。

```

*注意：配置了driver自动恢复机制时，如果想要根据旧的应用程序的checkpoint信息，启动新的应用程序，是不可行的。
需要让新的应用程序针对新的checkpoint目录启动，或者删除之前的checkpoint目录。


### 监控应用程序

* 当Spark Streaming应用启动时，Spark Web UI会显示一个独立的streaming tab，会显示Receiver的信息，比如是否活跃，接收到了多少数据，是否有异常等；
还会显示完成的batch的信息，batch的处理时间、队列延迟等。这些信息可以用于监控spark streaming应用的进度。

* Spark UI中，以下两个统计指标格外重要：

```markdown
1. 处理时间——每个batch的数据的处理耗时
2. 调度延迟——一个batch在队列中阻塞住，等待上一个batch完成处理的时间

```

* 如果batch的处理时间，比batch的间隔要长的话，而且调度延迟时间持续增长，应用程序不足以使用当前设定的速率来处理接收到的数据，此时，可以考虑增加batch的间隔时间。

---

## Spark Streaming容错机制以及事务语义详解

[Spark Streaming容错机制以及事务语义详解](https://blog.csdn.net/love__live1/article/details/86575828)

### 容错机制的背景

* 要理解Spark Streaming提供的容错机制，先回忆一下Spark RDD的基础容错语义：

>RDD，Ressilient Distributed Dataset，是不可变的、确定的、可重新计算的、分布式的数据集。每个RDD都会记住确定好的计算操作的血缘关系，
（val lines = sc.textFile(hdfs file);          
val words = lines.flatMap();        
val pairs = words.map();    
val wordCounts = pairs.reduceByKey()）        
这些操作应用在一个容错的数据集上来创建RDD。
如果因为某个Worker节点的失败（挂掉、进程终止、进程内部报错），导致RDD的某个partition数据丢失了，那么那个partition可以通过对原始的容错数据集应用操作血缘，来重新计算出来。
所有的RDD transformation操作都是确定的，最后一个被转换出来的RDD的数据，一定是不会因为Spark集群的失败而丢失的。

* Spark操作的通常是容错文件系统中的数据，比如HDFS。因此，所有通过容错数据生成的RDD也是容错的。
然而，对于Spark Streaming来说，这却行不通，因为在大多数情况下，数据都是通过网络接收的（除了使用fileStream数据源）。
要让Spark Streaming程序中，所有生成的RDD，都达到与普通Spark程序的RDD，相同的容错性，接收到的数据必须被复制到多个Worker节点上的Executor内存中，默认的复制因子是2。

* 基于上述理论，在出现失败的事件时，有两种数据需要被恢复：

```markdown
1. 数据接收到了，并且已经复制过——这种数据在一个Worker节点挂掉时，是可以继续存活的，因为在其他Worker节点上，还有它的一份副本。
2. 数据接收到了，但是正在缓存中，等待复制的——因为还没有复制该数据，因此恢复它的唯一办法就是重新从数据源获取一份。

```
此外，还有两种失败是我们需要考虑的：

```markdown
1. Worker节点的失败
——任何一个运行了Executor的Worker节点的挂掉，都会导致该节点上所有在内存中的数据都丢失。如果有Receiver运行在该Worker节点上的Executor中，那么缓存的，待复制的数据，都会丢失。

2. Driver节点的失败
——如果运行Spark Streaming应用程序的Driver节点失败了，那么显然SparkContext会丢失，那么该Application的所有Executor的数据都会丢失。
```

### Spark Streaming容错语义的定义

* 流式计算系统的容错语义，通常是以一条记录能够被处理多少次来衡量的。

有三种类型的语义可以提供：

```markdown
最多一次：每条记录可能会被处理一次，或者根本就不会被处理。可能有数据丢失。

至少一次：每条记录会被处理一次或多次，这种语义比最多一次要更强，因为它确保零数据丢失。但是可能会导致记录被重复处理几次。

一次且仅一次：每条记录只会被处理一次——没有数据会丢失，并且没有数据会处理多次。这是最强的一种容错语义。

```

### Spark Streaming的基础容错语义

* 在Spark Streaming中，处理数据都有三个步骤：

```markdown
接收数据：使用Receiver或其他方式接收数据。
计算数据：使用DStream的transformation操作对数据进行计算和处理。
推送数据：最后计算出来的数据会被推送到外部系统，比如文件系统、数据库等。
```

* 如果应用程序要求必须有一次且仅一次的语义，那么上述三个步骤都必须提供一次且仅一次的语义。每条数据都得保证，只能接收一次、只能计算一次、只能推送一次。

Spark Streaming中实现这些语义的步骤如下：

```markdown
接收数据：不同的数据源提供不同的语义保障。

计算数据：所有接收到的数据一定只会被计算一次，这是基于RDD的基础语义所保障的。即使有失败，只要接收到的数据还是可访问的，最后一个计算出来的数据一定是相同的。

推送数据：output操作默认能确保至少一次的语义，因为它依赖于output操作的类型，以及底层系统的语义支持（比如是否有事务支持等），但是用户可以实现它们自己的事务机制来确保一次且仅一次的语义。


```

### 接收数据的容错语义

* 基于文件的数据源
 
 如果所有的输入数据都在一个容错的文件系统中，比如HDFS，Spark Streaming一定可以从失败进行恢复，并且处理所有数据。这就提供了一次且仅一次的语义，意味着所有的数据只会处理一次。
 
* 基于Receiver的数据源

 对于基于Receiver的数据源，容错语义依赖于失败的场景和Receiver类型。
 
 ```markdown

 可靠的Receiver：
    这种Receiver会在接收到了数据，并且将数据复制之后，对数据源执行确认操作。
    如果Receiver在数据接收和复制完成之前，就失败了，那么数据源对于缓存的数据会接收不到确认，此时，当Receiver重启之后，数据源会重新发送数据，没有数据会丢失。

 不可靠的Receiver：
    这种Receiver不会发送确认操作，因此当Worker或者Driver节点失败的时候，可能会导致数据丢失。
 
```

* 不同的Receiver，提供了不同的语义。

如果Worker节点失败了，那么使用的是可靠的Receiver的话，没有数据会丢失。

使用的是不可靠的Receiver的话，接收到，但是还没复制的数据，可能会丢失。

如果Driver节点失败的话，所有过去接收到的，和复制过缓存在内存中的数据，全部会丢失。
 
* 要避免这种过去接收的所有数据都丢失的问题，Spark从1.2版本开始，引入了预写日志机制，可以将Receiver接收到的数据保存到容错存储中。

如果使用可靠的Receiver，并且还开启了预写日志机制，那么可以保证数据零丢失。这种情况下，会提供至少一次的保障。（Kafka是可以实现可靠Receiver的）

* 从Spark 1.3版本开始，引入了新的Kafka Direct API，可以保证，所有从Kafka接收到的数据，都是一次且仅一次。
基于该语义保障，如果自己再实现一次且仅一次语义的output操作，那么就可以获得整个Spark Streaming应用程序的一次且仅一次的语义。


### 输出数据的容错语义

* output操作，比如foreachRDD，可以提供至少一次的语义。
那意味着，当Worker节点失败时，转换后的数据可能会被写入外部系统一次或多次。对于写入文件系统来说，这还是可以接收的，因为会覆盖数据。但是要真正获得一次且仅一次的语义，有两个方法：

```markdown
幂等更新：
    多次写操作，都是写相同的数据，例如saveAs系列方法，总是写入相同的数据。
事务更新：
    所有的操作都应该做成事务的，从而让写入操作执行一次且仅一次。
    给每个batch的数据都赋予一个唯一的标识，然后更新的时候判定，如果数据库中还没有该唯一标识，那么就更新，如果有唯一标识，那么就不更新。
```

```markdown
dstream.foreachRDD { (rdd, time) =>
  rdd.foreachPartition { partitionIterator =>
    val partitionId = TaskContext.get.partitionId()
    val uniqueId = generateUniqueId(time.milliseconds, partitionId)
    // partitionId和foreachRDD传入的时间，可以构成一个唯一的标识
  }
}

```


### Storm的容错语义

* Storm首先，它可以实现消息的高可靠性，就是说，它有一个机制，叫做Acker机制，可以保证，如果消息处理失败，那么就重新发送。
保证了，至少一次的容错语义。但是光靠这个，还是不行，数据可能会重复。

* Storm提供了非常非常完善的事务机制，可以实现一次且仅一次的事务机制。

事务Topology、透明的事务Topology、非透明的事务Topology，可以应用各种各样的情况。

对实现一次且仅一次的这种语义的支持，做的非常非常好。用事务机制，可以获得它内部提供的一个唯一的id，然后基于这个id，就可以实现，output操作，
输出，推送数据的时候，先判断，该数据是否更新过，如果没有的话，就更新；如果更新过，就不要重复更新了。

* 所以，至少，在容错 / 事务机制方面，我觉得Spark Streaming还有很大的空间可以发展。特别是对于output操作的一次且仅一次的语义支持！

































