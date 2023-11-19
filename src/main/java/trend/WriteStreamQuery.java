package trend;

import com.typesafe.config.Config;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;

public class WriteStreamQuery {
    Dataset<Row> streamingDF;
    Config config;
    VoidFunction2 processBatch;
    StreamingQuery streamingQuery;

    public WriteStreamQuery (Dataset<Row> streamingDF, Config config, VoidFunction2 processBatch) throws TimeoutException {
        this.streamingDF = streamingDF;
        this.config = config;
        this.processBatch = processBatch;
        this.streamingQuery = startStreaming ();
    }

    private StreamingQuery startStreaming() throws TimeoutException {
        return streamingDF
                .writeStream()
                .option("checkpointLocation", config.getString("checkpointLocation"))
                .trigger(Trigger.ProcessingTime(config.getString("trigger.processingTime")))
                .outputMode(OutputMode.Append())
                .foreachBatch( processBatch )
                .start();
    }


    public StreamingQuery getStreamingQuery() {
        return streamingQuery;
    }
}
