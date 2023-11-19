package trend;

import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcType;
import org.apache.spark.sql.types.*;
import scala.Option;

import java.sql.Types;

public class OracleCustomDialect extends JdbcDialect {
    /**
     * this class is an alpha version to solve the problem of creating oracle table with the case-sensitive columns
     */
    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:oracle") || url.contains("oracle");
    }

    @Override
    // Handle NUMBER fields that have no precision/scale in special way because JDBC ResultSetMetaData converts this to 0 precision and -127 scale
    public Option<DataType> getCatalystType(int sqlType, String typeName, int size, MetadataBuilder md) {
        if (sqlType == Types.NUMERIC && size == 0) {
            return Option.apply(DecimalType.apply(DecimalType.MAX_PRECISION(), 10));
        } else {
            return Option.empty();
        }
    }

    @Override
    public Option<JdbcType> getJDBCType(DataType dt) {
        if (dt instanceof StringType) {
            return Option.apply(new JdbcType("VARCHAR2(2000)", Types.VARCHAR));
        } else if (dt instanceof BooleanType) {
            return Option.apply(new JdbcType("NUMBER(1)", Types.NUMERIC));
        } else if (dt instanceof IntegerType) {
            return Option.apply(new JdbcType("NUMBER(10)", Types.NUMERIC));
        } else if (dt instanceof LongType) {
            return Option.apply(new JdbcType("NUMBER(19)", Types.NUMERIC));
        } else if (dt instanceof DoubleType) {
            return Option.apply(new JdbcType("NUMBER(19,4)", Types.NUMERIC));
        } else if (dt instanceof FloatType) {
            return Option.apply(new JdbcType("NUMBER(19,4)", Types.NUMERIC));
        } else if (dt instanceof ShortType) {
            return Option.apply(new JdbcType("NUMBER(5)", Types.NUMERIC));
        } else if (dt instanceof ByteType) {
            return Option.apply(new JdbcType("NUMBER(3)", Types.NUMERIC));
        } else if (dt instanceof BinaryType) {
            return Option.apply(new JdbcType("BLOB", Types.BLOB));
        }else if (dt instanceof DecimalType) {
            return Option.apply(new JdbcType("NUMBER(38)", Types.NUMERIC));
        } else if (dt instanceof TimestampType) {
            return Option.apply(new JdbcType("DATE", Types.TIMESTAMP));
        } else if (dt instanceof DateType) {
            return Option.apply(new JdbcType("DATE", Types.DATE));
        } else {
            return Option.empty();
        }
    }

    @Override
    public String quoteIdentifier(String colName) {
        return colName;
    }
}