package trend;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.Map;


public class WriteStreamHandler {

    public static final String CONFIG_NAME_FORMAT = "format";
    public static final String CONFIG_NAME_JDBC = "jdbc";
    public static final String CONFIG_NAME_MODE = "mode";

    public void write(Dataset<Row> outputDF, Config config) {

        if (config.getString(CONFIG_NAME_FORMAT).equalsIgnoreCase(CONFIG_NAME_JDBC)) {
            Map<String, String> optionsMap = new HashMap<>();
            for (Map.Entry<String, ConfigValue> entry : config.entrySet()) {
                if (!entry.getKey().equalsIgnoreCase(CONFIG_NAME_MODE) && !entry.getKey().equalsIgnoreCase(CONFIG_NAME_JDBC))
                    optionsMap.put(entry.getKey(), entry.getValue().unwrapped().toString());
            }

            outputDF
                    .write()
                    .format(config.getString(CONFIG_NAME_FORMAT).toLowerCase())
                    .options(optionsMap)
                    .mode(config.getString(CONFIG_NAME_MODE))
                    .save();

        }

    }
}