package trend;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public class Aggregate {
    private final Dataset<Row> streamDF;
    private final String windowDuration;
    private final String watermarkDuration;


    public Aggregate(Dataset<Row> streamDF, String windowDuration, String watermarkDuration) {
        this.streamDF = streamDF;
        this.windowDuration = windowDuration;
        this.watermarkDuration = watermarkDuration;
    }

    public Dataset<Row> getAggregatedDF() {
        Dataset<Row> streamingDF = streamDF.withWatermark("timestamp", watermarkDuration);

        Column windowCol = window(
                col("timestamp"),
                windowDuration
        );

        Column groupedCol = col("Topic_Name");

        Dataset<Row> aggregatedDF = streamingDF
                .groupBy(windowCol, groupedCol)
                .agg(
                        count(groupedCol).as("Topic_Count")
                );

        return aggregatedDF;
    }
}
