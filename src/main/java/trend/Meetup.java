package trend;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import trend.event.MeetupEvent;
import org.apache.spark.sql.*;
import org.apache.spark.sql.jdbc.JdbcDialects;
import org.apache.spark.sql.streaming.*;



public final class Meetup {
    public static void main(String[] args) throws Exception {

        String filepath = "src/main/resources/application.conf";
        String env = "dev";
        ConfigHandler configHandler = new ConfigHandler(filepath, env);
        Config config = configHandler.getConfig();

        SparkSession spark = new SparkSessionHandler(config.getConfig("SparkSession")).getSparkSession();
        InputStreamHandler inputStreamHandler = new InputStreamHandler(config.getConfig("Extract.meetup"), spark);

        // register a custom jdbc dialect for oracle database to solve the problem of creating
        // oracle table by spark in case the table is not exist and spark will create table
        JdbcDialects.registerDialect(new OracleCustomDialect());

        Dataset<Row> inputStreamDF = inputStreamHandler.getInputStreamDF();

        ObjectMapper mapper = new ObjectMapper();

        String jsonString = mapper.writeValueAsString(new MeetupEvent());

        Dataset<Row> dataFrameWithSchema = new ApplySchema(inputStreamDF, spark, jsonString).getDFWithScheme();

        Dataset<Row> aggregatedWindowedDF = new Aggregate(dataFrameWithSchema,
                config.getString("options.window.duration"),
                config.getString("options.watermark.duration")
        ).getAggregatedDF();

        WriteStreamHandler writeStreamHandler = new WriteStreamHandler();
        ProcessBatch processBatch = new ProcessBatch( spark , config, writeStreamHandler);

        StreamingQuery query = new WriteStreamQuery(aggregatedWindowedDF, config.getConfig("options"), processBatch).getStreamingQuery();

        query.awaitTermination();

    }

}