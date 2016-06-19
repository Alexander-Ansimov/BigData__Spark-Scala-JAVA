package football;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.functions;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

import static football.DataFrameCreator.LINE;
import static org.apache.spark.sql.functions.*;

/**
 * Created by Evegeny on 11/06/2016.
 */
@Component
public class LineLengthValidator implements LineValidator {


    @Override
    public DataFrame validate(DataFrame dataFrame) {
        return dataFrame.withColumn("errorInLength", callUDF(getClass().getSimpleName(),column(DataFrameCreator.LINE)));
    }

    @Override
    public String call(String line) throws Exception {
        if (line == null || line.isEmpty()) {
            return "line is emty";
        }
        return null;
    }
}
