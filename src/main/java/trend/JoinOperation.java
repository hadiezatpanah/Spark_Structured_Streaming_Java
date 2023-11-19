package trend;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.trim;


public class JoinOperation {
    public static class JoinOperationUtils {

        /**
         * Apply join operation on two DataFrames.
         *
         * @param batchDF          Current Batch DataFrame
         * @param trendDf          Trend DataFrame
         * @param batchJoinColumn  Column to join on in batch DataFrame
         * @param trendDfColumn    Column to join on in trend DataFrame
         * @param joinType         Type of join (e.g., "inner", "left_outer")
         * @return Joined DataFrame
         */
        public static Dataset<Row> apply(Dataset<Row> batchDF, Dataset<Row> trendDf, String batchJoinColumn, String trendDfColumn, String joinType) {
            JoinWithTrendStat joinWithTrendStat = new JoinWithTrendStat(batchDF, trendDf, batchJoinColumn, trendDfColumn, joinType);
            return joinWithTrendStat.getJoinedDF()
                    .select(
                            functions.when(col("Topic_Name").isNull(), col("Topic_NameTDF")).otherwise(col("Topic_Name")).as("Topic_Name"),
                            functions.coalesce(col("Topic_Count"), functions.lit(0)).as("Topic_Count"),
                            functions.coalesce(col("avg"), functions.lit(0)).as("avg"),
                            functions.coalesce(col("sqr_Avg"), functions.lit(0)).as("sqr_Avg")
                    );
        }
    }
    private static class JoinWithTrendStat {
        private final Dataset<Row> streamDF;
        private final Dataset<Row> trendDf;
        private final String streamJoinColumn;
        private final String trendDfJoinColumn;
        private final String joinType;

        public JoinWithTrendStat(Dataset<Row> streamDF, Dataset<Row> trendDf, String streamJoinColumn, String trendDfJoinColumn, String joinType) {
            this.streamDF = streamDF;
            this.trendDf = trendDf;
            this.streamJoinColumn = streamJoinColumn;
            this.trendDfJoinColumn = trendDfJoinColumn;
            this.joinType = joinType;
        }

        public Dataset<Row> getJoinedDF() {

            Column joinCondition = trim(streamDF.col(streamJoinColumn))
                    .equalTo(trim(trendDf.col(trendDfJoinColumn)));

            return streamDF.join(trendDf, joinCondition, joinType)
                    .select(
                            streamDF.col("*"),
                            trendDf.col(trendDfJoinColumn).as(trendDfJoinColumn + "TDF"),
                            trendDf.col("avg"),
                            trendDf.col("sqr_Avg")
                    );
        }
    }
}