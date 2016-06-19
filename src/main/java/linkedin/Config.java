package linkedin;

import org.apache.spark.SparkConf;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class Config {

    @Bean
    public SparkConf sparkConf() {
        return new SparkConf().setAppName("my json").setMaster("local");
    }

}
