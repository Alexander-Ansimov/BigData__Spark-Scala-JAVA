package football;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by Evegeny on 11/06/2016.
 */
@Service
public class DataFrameCreator {
    public static final String LINE = "line";
    @Autowired
    private SQLContext sqlContext;

    @Autowired
    private JavaSparkContext sc;

    public DataFrame create(String pathToFile) {
        JavaRDD<String> rdd = sc.textFile(pathToFile);
        JavaRDD<Row> rddOfRows = rdd.map(RowFactory::create);
        return sqlContext.createDataFrame(rddOfRows, DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField(LINE, DataTypes.StringType, true)
        }));
    }
}





