package songs;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * Created by Evegeny on 14/07/2016.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = SpringConfig.class)
public class PopularWordsServiceImplTest {

    @Autowired
    private JavaSparkContext sc;
    @Autowired
    private PopularWordsService popularWordsService;

    @Test
    public void testTopX() throws Exception {
        JavaRDD<String> rdd = sc.parallelize(Arrays.asList("scala,java,java,groovy"));
        String mostPopular = popularWordsService.topX(rdd, 1).get(0);
        Assert.assertEquals("java",mostPopular);
    }
}




