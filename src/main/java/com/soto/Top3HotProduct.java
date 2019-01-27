package com.soto;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * 每隔10s统计最近60s的每个种类的每个商品的点击次数,然后统计出每个各类top3的热门商品
 */
public class Top3HotProduct {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Top3HotProduct")
                .setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        //输入日志的格式
        //leo product1 category1
        //leo iphone mobile_phone
        JavaReceiverInputDStream<String> productClickLogsDStream = jssc.socketTextStream("localhost", 9999);

        //映射为(category_product1,1),从而后面使用window操作,对窗口中的这种格式的数据,进行reduceByKey操作,从而统计出来一个窗口的每个
        //种类的每个商品的点击次数
        JavaPairDStream<String, Integer> categoryProductPairsDStream = productClickLogsDStream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s.split(" ")[2] + "_" + s.split(" ")[1], 1);
            }
        });


        //执行window操作,得到60s内RDD的--> (category_product,counts)
        JavaPairDStream<String,Integer> categoryProductCountsDStream = categoryProductPairsDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, Durations.seconds(60), Durations.seconds(10));



        //foreachRDD 在内部使用Spark SQL执行热门商品的统计
        categoryProductCountsDStream.foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Integer> categoryProductCountRDD) throws Exception {

                //转换为JavaRDD<Row>的格式以便在后面进行SparkSql操作-->Row(category, product, count)
                JavaRDD<Row> categoryProductCountRowRDD = categoryProductCountRDD.map(new Function<Tuple2<String, Integer>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Integer> tuple) throws Exception {
                        String category = tuple._1.split("_")[0];
                        String product = tuple._1.split("_")[1];
                        Integer count = tuple._2;
                        return RowFactory.create(category, product, count);
                    }
                });


                //执行DataFrame转换
                List<StructField> structFields = new ArrayList<>();
                structFields.add(DataTypes.createStructField("category", DataTypes.StringType, true));
                structFields.add(DataTypes.createStructField("product", DataTypes.StringType, true));
                structFields.add(DataTypes.createStructField("click_count", DataTypes.IntegerType, true));

                StructType structType = DataTypes.createStructType(structFields);
                HiveContext hiveContext = new HiveContext(categoryProductCountRDD.context());
                DataFrame categoryProductCountDF = hiveContext.createDataFrame(categoryProductCountRowRDD, structType);

                //将60s内的每个种类的每个商品的点击次数注册为临时表
                categoryProductCountDF.registerTempTable("product_click_log");

                //执行sql语句,针对临时表,统计出来每个种类下点击次数前3的热门商品
                DataFrame top3ProductDF = hiveContext.sql(
                        " select category,product,click_count " +
                                " from ( " +
                                " select " +
                                " category, " +
                                "product, " +
                                "click_count, " +
                                "row_number() over (partition by category order by click_count desc ) rank " +
                                "from product_click_log " +
                                " ) tmp " +
                                " where rank <= 3 "
                );

                top3ProductDF.show();

                return null;
            }
        });



        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
