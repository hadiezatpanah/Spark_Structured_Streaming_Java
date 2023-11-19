package trend;

import com.typesafe.config.Config;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.reflect.ClassTag;

import static org.apache.spark.sql.functions.*;


public class ProcessBatch implements VoidFunction2<Dataset<Row>, Long> {
    private final SparkSession spark;
    private final String checkpointLocation;
    private final double trend_decay;
    private final int trendOutputNumber;
    private Config config;
    private WriteStreamHandler writeStreamHandler;

    public ProcessBatch(SparkSession spark, Config config, WriteStreamHandler writeStreamHandler) {
        this.spark = spark;
        this.config = config;
        this.checkpointLocation = config.getString("options.checkpointLocation");
        this.trend_decay = config.getDouble("Load.trend_decay");
        this.trendOutputNumber = config.getInt("Load.trendOutputNumber");
        this.writeStreamHandler = writeStreamHandler;
    }


    @Override
    public void call(Dataset<Row> batchDF, Long batchId) {
        batchDF.show(false);
        if ( ! batchDF.isEmpty()) {
            batchDF.persist();

            Dataset<Row> trendTable;
            if ( ! spark.catalog().tableExists("trend_table") ) {
                ClassTag<Row> rowTag = scala.reflect.ClassTag$.MODULE$.apply(Row.class);
                trendTable = spark.createDataFrame(spark.sparkContext().emptyRDD(rowTag), getTrendTableSchema());
            } else  {

                spark.sparkContext().setCheckpointDir(checkpointLocation);
                spark.sql("refresh TABLE trend_table");
                trendTable  = spark.read().table("trend_table").checkpoint();
            }

            Dataset<Row> joinedDF = JoinOperation.JoinOperationUtils.apply(batchDF, trendTable, "Topic_Name", "Topic_Name", "fullouter");

            Dataset<Row> processedDF = joinedDF.select(
                    when((col("avg").equalTo(lit(0)).and(col("sqr_Avg").equalTo(lit(0)))),
                            col("Topic_Count"))
                            .otherwise(col("avg").multiply(lit(trend_decay)).plus(
                                    col("Topic_Count").multiply(lit(1 - trend_decay))))
                            .as("avg"),
                    when((col("avg").equalTo(lit(0)).and(col("sqr_Avg").equalTo(lit(0)))),
                            pow(col("Topic_Count"), lit(2)).cast("decimal(38,3)"))
                            .otherwise(col("sqr_Avg").multiply(lit(trend_decay)).plus(
                                    pow(col("Topic_Count"), lit(2)).cast("decimal(38,3)").multiply(
                                            lit(1 - trend_decay))))
                            .as("sqr_Avg"),
                    col("Topic_Name"),
                    col("Topic_Count")
            );

            processedDF.select("Topic_Name", "avg", "sqr_Avg")
                    .write()
                    .mode("overwrite")
                    .saveAsTable("trend_table");

            WindowSpec windowSpec = Window.orderBy(col("Trend_Score").desc());
            Dataset<Row> finalDF = processedDF.select(col("Topic_Name"),
                    when(sqrt(pow(col("avg").minus(col("sqr_Avg")), 2).cast("decimal(38,3)")).equalTo(lit(0).cast("decimal(38,3)")),
                    col("Topic_Count").minus(col("avg")))
                    .otherwise((col("Topic_Count").minus(col("avg"))).divide(
                            sqrt(pow(col("avg").minus(col("sqr_Avg")), 2)).cast("decimal(38,3)")))
                            .alias("Trend_Score")
            );

            Dataset<Row> trendDF = finalDF.withColumn("Trend_Rank", row_number().over(windowSpec).cast("integer"))
                    .orderBy(col("Trend_Rank").asc())
                    .withColumn("Process_DataTime", current_timestamp())
                    .filter(col("Trend_Rank").leq(trendOutputNumber));

            trendDF.show(false);
            writeStreamHandler.write(trendDF, config.getConfig("Load.oracle"));


            batchDF.unpersist();
        }
    }
    private StructType getTrendTableSchema () {
        return
            new StructType(new StructField[]{
            new StructField("Topic_Name", DataTypes.StringType, true, Metadata.empty()),
            new StructField("avg", DataTypes.createDecimalType(38, 3), true, Metadata.empty()),
            new StructField("sqr_Avg", DataTypes.createDecimalType(38, 3), true, Metadata.empty())
        });
    }
}