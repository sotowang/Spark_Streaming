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










