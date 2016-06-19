package countWords;

import countWords.config.RootConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by Evegeny on 11/06/2016.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = RootConfig.class)
public class PopularWordCounterTest {
    @Autowired
    private PopularWordCounter wordCounter;

    @Autowired
    private JavaSparkContext sc;

    @Test
    public void testTopX() throws Exception {
        List<String> list = Arrays.asList("Java the java the java, the", "Scala the Groovy the groovy", "pascal kotlin");
        JavaRDD<String> rdd = sc.parallelize(list);
        List<String> popularWords = wordCounter.topX(rdd, 1);
        Assert.assertEquals("java", popularWords.get(0));

    }
}