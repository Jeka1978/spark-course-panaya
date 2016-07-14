package songs;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by Evegeny on 14/07/2016.
 */
@Service
public class MusicJudge {
    @Autowired
    private PopularWordsService popularWordsService;

    @Autowired
    private JavaSparkContext sc;


    public List<String> topX(String path, int topX) {
        JavaRDD<String> rdd = sc.textFile(path);
        return popularWordsService.topX(rdd, topX);
    }










}
