package songs;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.context.annotation.*;

/**
 * Created by Evegeny on 14/07/2016.
 */
@Configuration
@PropertySource("classpath:user.properties")
@ComponentScan
public class SpringConfig {
    @Bean
    public SparkConf sparkConf(){
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("songs analytic");
        return conf;
    }
    @Bean
    public JavaSparkContext sc(){
        return new JavaSparkContext(sparkConf());
    }


}






