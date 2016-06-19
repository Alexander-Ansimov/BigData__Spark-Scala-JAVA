package linkedin;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructField;
import org.junit.Test;

import java.util.Arrays;

/**
 * Created by alex on 11.06.16.
 */
public class JsonUtil {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("my json").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame dataFrame = sqlContext.read().json("data/*.json");

        dataFrame.show();
        dataFrame.schema().printTreeString();
        dataFrame.printSchema();

        StructField[] fields = dataFrame.schema().fields();
        Arrays.asList(fields).stream().map(StructField::dataType).forEach(System.out::println);

        DataFrame withSalary = dataFrame.withColumn("new column", functions.size(functions.column("keywords"))
                .multiply(functions.column("age")).multiply(10)).drop("age");

        withSalary.show();


        DataFrame word = dataFrame.select(functions.explode(functions.column("keywords")).as("word"));
        word.show();




    }

}
