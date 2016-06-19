package countWords.config;

import org.apache.spark.SparkConf;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by Evegeny on 11/06/2016.
 */
@Configuration
@Windows(true)
public class ProdConfig {
    @Bean
    public SparkConf sparkConf(){
        return new SparkConf().setAppName("my app");
    }
}
