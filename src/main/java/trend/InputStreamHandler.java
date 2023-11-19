package trend;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;

import java.util.HashMap;
import java.util.Map;


public class InputStreamHandler {
    private Dataset<Row> inputStreamDF;
    private final String FORMAT_KEY = "format";

    public InputStreamHandler(Config config, SparkSession sparkSession) {
        DataStreamReader reader = sparkSession.readStream().format(config.getString(FORMAT_KEY));

        Map<String, String> optionsMap = new HashMap<>();
        for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
            if (!entry.getKey().equals(config.getString(FORMAT_KEY)))
                optionsMap.put(entry.getKey(), entry.getValue().unwrapped().toString());
        }

        inputStreamDF = reader.options(optionsMap).load();
    }


    public Dataset<Row> getInputStreamDF() {
        return inputStreamDF;
    }
}
