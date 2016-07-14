package songs;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Evegeny on 14/07/2016.
 */
@Service
public class PopularWordsServiceImpl implements PopularWordsService, Serializable {

    @Autowired
    private UserProperties properties;


    @AutowiredBroadcast(UserProperties.class)
    private Broadcast<UserProperties> userPropertiesBroadcast;


    @Override
    public List<String> topX(JavaRDD<String> rdd,int x) {
        return rdd.map(String::toLowerCase)
                .flatMap(WordUtils::getWords)
                .filter(word -> !userPropertiesBroadcast.value().garbage.contains(word))
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .map(Tuple2::_2).take(x);

    }
}
