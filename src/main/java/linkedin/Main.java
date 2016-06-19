package linkedin;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.explode;

/**
 * Created by Evegeny on 11/06/2016.
 */
public class Main {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("linked in").setMaster("local[1]");
        SparkContext sparkContext = new SparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(sparkContext);
        DataFrame linkedIn = sqlContext.read().json("data/linkedin/*.json");
        linkedIn.show();

        DataFrame withSalary = linkedIn.
                withColumn("salary", size(column("keywords"))
                        .multiply(column("age")).multiply(10)).drop("age");
        withSalary.show();

        DataFrame keywords = linkedIn.
                select(explode(column("keywords")).as("keyword"));
        keywords.show();

        DataFrame orderedBy = keywords.groupBy("keyword")
                .agg(count("keyword").as("amount"))
                .orderBy(column("amount").desc());
        orderedBy.show();

        String mostPopularWord = orderedBy.first().getString(orderedBy.schema().fieldIndex("keyword"));
        System.out.println("mostPopularWord = " + mostPopularWord);

        withSalary.where(
                column("salary").leq(1200).and(array_contains(column("keywords"),mostPopularWord)))
                .select("name","salary").show();

    }

}
