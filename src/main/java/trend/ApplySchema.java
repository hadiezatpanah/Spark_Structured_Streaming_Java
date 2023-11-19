package trend;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class ApplySchema {
    Dataset<Row> DFWithScheme;

    public ApplySchema(Dataset<Row> inputStreamDF, SparkSession spark, String jsonSchema) {

        DFWithScheme = inputStreamDF
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
                .select(from_json(col("value"), getSchema(spark, jsonSchema)).as("data"), col("timestamp"))
                .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyyMMddHHmmss"))
                .selectExpr("explode(data.group.group_topics.topic_name) as Topic_Name",
                        "data.group.group_city as Group_City",
                        "data.group.group_country as Group_Country",
                        "timestamp");
    }
    private static StructType getSchema(SparkSession spark, String jsonString) {
        List<String> jsonData = Arrays.asList(jsonString);
        Dataset<String> dataset = spark.createDataset(jsonData, Encoders.STRING());
        Dataset<Row> dfWithSchema = spark.read().json(dataset);
        return dfWithSchema.schema();
    }

    public Dataset<Row> getDFWithScheme() {
        return DFWithScheme;
    }

}
