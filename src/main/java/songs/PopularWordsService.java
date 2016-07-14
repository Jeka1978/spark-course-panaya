package songs;

import org.apache.spark.api.java.JavaRDD;

import java.util.List;

/**
 * Created by Evegeny on 14/07/2016.
 */
public interface PopularWordsService {
    List<String> topX(JavaRDD<String> rdd,int x);
}
