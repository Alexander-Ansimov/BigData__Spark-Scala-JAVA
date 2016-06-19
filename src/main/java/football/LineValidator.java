package football;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.api.java.UDF1;

/**
 * Created by Evegeny on 11/06/2016.
 */
public interface LineValidator extends UDF1<String,String>{
    DataFrame validate(DataFrame dataFrame);
}
