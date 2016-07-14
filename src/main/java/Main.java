import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.fluttercode.datafactory.impl.DataFactory;

/**
 * Created by Evegeny on 14/07/2016.
 */
public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("taxi");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile("data/taxi/trips.txt");
        System.out.println(rdd.count());
        Accumulator<Integer> shortDistance = sc.accumulator(0, "short");
        Accumulator<Integer> avgDistance = sc.accumulator(0, "avg");
        Accumulator<Integer> longDistance = sc.accumulator(0, "long");

    }
}
