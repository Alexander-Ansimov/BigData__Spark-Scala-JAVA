package countWords;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Evegeny on 11/06/2016.
 */
@Service
public class PopularWordCounter implements Serializable {


    @AutowiredBroadcast
    private Broadcast<UserConfig> userConfig;

    public List<String> topX(JavaRDD<String> rdd, int x) {
        return rdd.map(String::toLowerCase).
                flatMap(WordsUtil::getWords)
                .filter(word -> !userConfig.value().garbage.contains(word))
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .map(Tuple2::_2).take(x);

    }
}
