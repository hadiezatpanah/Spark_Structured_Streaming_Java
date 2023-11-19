# Spark Structured Streaming - Java - Trend topics

## Context

This Spark Java project serves as a demonstration of a Java Spark solution and can be used as a template for any Spark Structured Streaming development using Java. The project focuses on identifying trending topics within each time window of streaming transaction data. In this solution, the issue of creating a table with case-sensitive columns (in the scenario where the table doesn't exist or when writing the table in overwrite mode) in Oracle has been addressed by developing a custom Oracle dialect and registering it.

## Datasets
The input stream consists of JSON-format data from a Kafka broker with the following schema:

```
root
 |-- venue: struct (nullable = true)
 |    |-- venue_name: string (nullable = true)
 |    |-- lon: double (nullable = true)
 |    |-- lat: double (nullable = true)
 |    |-- venue_id: integer (nullable = true)
 |-- visibility: string (nullable = true)
 |-- response: string (nullable = true)
 |-- guests: integer (nullable = true)
 |-- member: struct (nullable = true)
 |    |-- member_id: integer (nullable = true)
 |    |-- photo: string (nullable = true)
 |    |-- member_name: string (nullable = true)
 |-- rsvp_id: long (nullable = true)
 |-- mtime: long (nullable = true)
 |-- event: struct (nullable = true)
 |    |-- event_name: string (nullable = true)
 |    |-- event_id: string (nullable = true)
 |    |-- time: long (nullable = true)
 |    |-- event_url: string (nullable = true)
 |    |-- group_topics: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- GroupTopic: struct (nullable = true)
 |    |    |    |    |-- urlkey: string (nullable = true)
 |    |    |    |    |-- topic_name: string (nullable = true)
 |-- group: struct (nullable = true)
 |    |-- group_topics: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- GroupTopic: struct (nullable = true)
 |    |    |    |    |-- urlkey: string (nullable = true)
 |    |    |    |    |-- topic_name: string (nullable = true)
 |    |-- group_city: string (nullable = true)
 |    |-- group_country: string (nullable = true)
 |    |-- group_id: integer (nullable = true)
 |    |-- group_name: string (nullable = true)
 |    |-- group_lon: double (nullable = true)
 |    |-- group_urlname: string (nullable = true)
 |    |-- group_lat: double (nullable = true)

```


## Requirements

* A distributable solution that calculate the current trend topics based on the history


# Solution
## Description

The solution leverages the modified z-score algorithm to calculate the treand topics over time by processing each window-batch of data. This algorithmic approach not only identifies trending topics but also adapts to changing patterns over time by utilizing decay factors. The modified z-score provides a robust mechanism for ranking topics within streaming data, offering valuable insights into emerging trends and shifts in user interests. The iterative process of updating the trend table ensures the algorithm's responsiveness to evolving data patterns. The resulting trend DataFrame serves as a dynamic record of ranked topics, empowering users to make informed decisions based on real-time streaming analytics.
### Ranking Algorithm (modified Z-Score)

>**if** this is first batch to be processed in kafka:  
>&nbsp;&nbsp;&nbsp;&nbsp;**avg** = topic_count and sqrAvg=topic_count <sup>2</sup> for each row in the microbatch DF;  
>&nbsp;&nbsp;&nbsp;&nbsp;save MicroBatch df in **TrendTable** in Spark warehouse;  
>**else**  
>&nbsp;&nbsp;&nbsp;&nbsp;retrieve **trend_table** from DB;  
>&nbsp;&nbsp;&nbsp;&nbsp;full outer join **trend_table** and **microbatchDF**  
>&nbsp;&nbsp;&nbsp;&nbsp;update avg and sqrAvg in trend_table based on new observation in microbatchDF:  
>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**avg** = avg * decay + topic_count * (1 - decay)  
>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**sqrAvg** = sqrAvg * decay + (topic_count <sup>2</sup>) * (1 - decay)  
>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Overwrite **new trend_table** based on new avg and sqrAvg in DB;  
>&nbsp;&nbsp;&nbsp;&nbsp;compute **trendDF** based on new avg and sqrAvg and topic observation:  
>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**topic_score** = (obs - avg) / std();  
>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;compute final ranked df based on each topic score and store the result in oracle; 

### Oracle Custom Dialect
1. **Usage Overview:**
    The `OracleCustomDialect` class is designed to enhance compatibility when working with Oracle databases in Spark SQL. It addresses issues related to case-sensitive column creation and extends the `JdbcDialect` class for Oracle-specific handling.

2. **Integration Steps:**
    To use the `OracleCustomDialect` in your Spark application, include the class in your project and register it as a custom dialect using the `spark.sql.extensions` configuration. For example:
    ```java
    SparkSession spark = SparkSession.builder().appName("YourAppName").getOrCreate();
    JdbcDialects.registerDialect(new OracleCustomDialect());
    ```

3. **Consideration:**
    Note that the `OracleCustomDialect` is an alpha version, and its usage should be validated through testing in your specific Spark SQL scenarios. Adapt the dialect to your Oracle database configurations and verify its effectiveness in addressing case-sensitive column challenges.


## Version Compatibility

| Java | Spark | Gradle | Kafka |
| ---- | ----- | ------ | ----- |
| 8 or greater | 3 or greater | 6.7 | 3 or greater |

## Configuration
Adjust the configuration settings in `application.conf` to customize the streaming window duration, oracle configuration or other relevant parameters.

## Getting Started
### Run
Start the Kafka server and create the relevant topic. Execute `Producer.java`, passing the path to the JSON file located in the resource folder. Once completed the second step, the main application `Meetup.java` is ready to be launched.

## Contributing
Contributions are welcome! If you have any ideas, suggestions, or bug fixes, feel free to submit a pull request.

## License
This project is licensed under the MIT License.

## Contact
For any inquiries or support, please contact `hadi.ezatpanah@gmail.com`.

This is just a template, so make sure to customize it with the appropriate details specific to your project.

## Author

👤 **Hadi Ezatpanah**

- Github: [@hadiezatpanah](https://github.com/hadiezatpanah)

## Version History
* 0.1
    * Initial Release
