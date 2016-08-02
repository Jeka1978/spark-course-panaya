package slidingWindow;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Evegeny on 02/08/2016.
 */
public class TestForShay {
    @Test
    public void testTransactionLog() throws InterruptedException {
        System.setProperty("hadoop.home.dir", "C:\\util\\hadoop-common-2.2.0-bin-master\\");
        SparkConf conf = new SparkConf().setAppName("for shay").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(1, 2, "ynet", "hey teachers"));
        rows.add(RowFactory.create(2, 1, "ynet", "I want"));
        rows.add(RowFactory.create(3, 2, "ynet", "leave the kids alone"));
        rows.add(RowFactory.create(4, 1, "ynet", "to be"));
        rows.add(RowFactory.create(5, 2, "google", "hello, hello"));
        rows.add(RowFactory.create(6, 1, "ynet", "free"));
        rows.add(RowFactory.create(7, 1, "cnn", "one of this days"));
        rows.add(RowFactory.create(8, 2, "google", "is there anybody"));
        rows.add(RowFactory.create(9, 1, "cnn", "I gonna cut you to a little peaces"));
        rows.add(RowFactory.create(10, 1, "ynet", "dark side"));
        rows.add(RowFactory.create(11, 2, "google", "in there"));
        rows.add(RowFactory.create(12, 1, "ynet", "of the moon"));

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("userID", DataTypes.IntegerType, false),
                DataTypes.createStructField("browser", DataTypes.StringType, false),
                DataTypes.createStructField("data", DataTypes.StringType, false)
        });

        // SC is a SparkContext or JavaSparkContext
        // This example REQUIRES HiveContext
        HiveContext hiveContext = new HiveContext(sc);

        // 0. Prepare initial data
        DataFrame dataFrame = hiveContext.createDataFrame(rows, schema);
        // dataFrame.show(false);
        // +---+------+-------+----------------------------------+
        // |id |userID|browser|data                              |
        // +---+------+-------+----------------------------------+
        // |1  |2     |ynet   |hey teachers                      |
        // |2  |1     |ynet   |I want                            |
        // |3  |2     |ynet   |leave the kids alone              |
        // |4  |1     |ynet   |to be                             |
        // |5  |2     |google |hello, hello                      |
        // |6  |1     |ynet   |free                              |
        // |7  |1     |cnn    |one of this days                  |
        // |8  |2     |google |is there anybody                  |
        // |9  |1     |cnn    |I gonna cut you to a little peaces|
        // |10 |1     |ynet   |dark side                         |
        // |11 |2     |google |in there                          |
        // |12 |1     |ynet   |of the moon                       |
        // +---+------+-------+----------------------------------+

        // 1. Add column that holds 'browser' value for previous row within same "userID"

        // SQL representation (PostgreSQL syntax):
        //
        //  SELECT
        //	    id, "userID", browser, data,
        //	    lag(browser, 1, null) OVER (PARTITION BY "userID" ORDER BY id) as browserSeq
        //  FROM public."inputTable"

        Column previousBrowserByUserID = functions
                .lag(functions.column("browser"), 1, null)
                .over(
                        Window
                                .partitionBy(functions.column("userID"))
                                .orderBy(functions.column("id"))
                );
        DataFrame browserSeqDF = dataFrame.withColumn("browserSeq", previousBrowserByUserID);
        // Order by id just for presentation purpose (should be avoided in production)
        // browserSeqDF.orderBy("id").show();
        //+---+------+-------+--------------------+----------+
        //| id|userID|browser|                data|browserSeq|
        //+---+------+-------+--------------------+----------+
        //|  1|     2|   ynet|        hey teachers|      null|
        //|  2|     1|   ynet|              I want|      null|
        //|  3|     2|   ynet|leave the kids alone|      ynet|
        //|  4|     1|   ynet|               to be|      ynet|
        //|  5|     2| google|        hello, hello|      ynet|
        //|  6|     1|   ynet|                free|      ynet|
        //|  7|     1|    cnn|    one of this days|      ynet|
        //|  8|     2| google|    is there anybody|    google|
        //|  9|     1|    cnn|I gonna cut you t...|       cnn|
        //| 10|     1|   ynet|           dark side|       cnn|
        //| 11|     2| google|            in there|    google|
        //| 12|     1|   ynet|         of the moon|      ynet|
        //+---+------+-------+--------------------+----------+

        // 2. Add column 'newSession' which contains 1 if 'browser' value is different from the previous 'browser'
        //    within same 'userID'. This means a start of a new session for this 'userID'.

        // SQL representation (PostgreSQL syntax):
        //
        // SELECT *,
        //	CASE
        //		WHEN
        //          browserSeq IS NULL OR browserSeq <> browser
        //      THEN 1
        //		ELSE 0
        //	END as newSession
        // FROM browserSeqDF
        Column newSessionExpression = functions
                .when(
                        functions
                                .column("browserSeq").isNull()
                                .or(
                                        functions.not(
                                                functions.column("browserSeq")
                                                        .equalTo(functions.column("browser"))
                                        )
                                ),
                        1
                )
                .otherwise(0);
        DataFrame sessionTransitionDF = browserSeqDF.withColumn("newSession", newSessionExpression);
        // Order by id just for presentation purpose (should be avoided in production)
        // sessionTransitionDF.orderBy("id").show();
        //+---+------+-------+--------------------+----------+----------+
        //| id|userID|browser|                data|browserSeq|newSession|
        //+---+------+-------+--------------------+----------+----------+
        //|  1|     2|   ynet|        hey teachers|      null|         1|
        //|  2|     1|   ynet|              I want|      null|         1|
        //|  3|     2|   ynet|leave the kids alone|      ynet|         0|
        //|  4|     1|   ynet|               to be|      ynet|         0|
        //|  5|     2| google|        hello, hello|      ynet|         1|
        //|  6|     1|   ynet|                free|      ynet|         0|
        //|  7|     1|    cnn|    one of this days|      ynet|         1|
        //|  8|     2| google|    is there anybody|    google|         0|
        //|  9|     1|    cnn|I gonna cut you t...|       cnn|         0|
        //| 10|     1|   ynet|           dark side|       cnn|         1|
        //| 11|     2| google|            in there|    google|         0|
        //| 12|     1|   ynet|         of the moon|      ynet|         0|
        //+---+------+-------+--------------------+----------+----------+


        // 3. Order by users (userID column),
        //    than by historical order (id column),
        //    than by transactionID.
        // 3.1. TransactionID is computed by summing up the "newSession" column
        //      within window data set
        //      sorted by userID and id
        //      and the window itself is limited from the start of the data up-to the current row

        // SQL representation (PostgreSQL syntax):
        //
        // SELECT *, SUM(newSession) OVER (
        //  	ORDER BY "userID", id
        //  	ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        // ) as transactionID
        // FROM sessionTransitionDF
        // ORDER BY "userID", id, transactionID

        DataFrame finalDF = sessionTransitionDF
                .withColumn("transactionID",
                        functions
                                .sum("newSession")
                                .over(
                                        Window
                                                .orderBy("userID", "id")
                                                .rowsBetween(Long.MIN_VALUE, 0)
                                )
                )
                // Select only required columns (that is - drop "browserSeq" and "newSession" columns)
                .select("id", "userID", "browser", "data", "transactionID");
        // Ordering is added just for presentation purpose (should be avoided in production)
        finalDF.orderBy("userID", "id", "transactionID").show(false);

        //+---+------+-------+----------------------------------+-------------+
        //|id |userID|browser|data                              |transactionID|
        //+---+------+-------+----------------------------------+-------------+
        //|2  |1     |ynet   |I want                            |1            |
        //|4  |1     |ynet   |to be                             |1            |
        //|6  |1     |ynet   |free                              |1            |
        //|7  |1     |cnn    |one of this days                  |2            |
        //|9  |1     |cnn    |I gonna cut you to a little peaces|2            |
        //|10 |1     |ynet   |dark side                         |3            |
        //|12 |1     |ynet   |of the moon                       |3            |
        //|1  |2     |ynet   |hey teachers                      |4            |
        //|3  |2     |ynet   |leave the kids alone              |4            |
        //|5  |2     |google |hello, hello                      |5            |
        //|8  |2     |google |is there anybody                  |5            |
        //|11 |2     |google |in there                          |5            |
        //+---+------+-------+----------------------------------+-------------+
    }
}
