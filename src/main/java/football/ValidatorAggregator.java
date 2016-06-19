package football;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * Created by Evegeny on 11/06/2016.
 */
@Service
public class ValidatorAggregator {
    @Autowired
    private List<LineValidator> lineValidators;

    @Autowired
    private SQLContext sqlContext;


    @PostConstruct
    public void registerUdfForAllValidators() {
        for (LineValidator lineValidator : lineValidators) {
            sqlContext.udf().register(lineValidator.getClass().getSimpleName(), lineValidator, DataTypes.StringType);
        }
    }

    public DataFrame validate(DataFrame dataFrame) {
        dataFrame.show();
        for (LineValidator lineValidator : lineValidators) {
            dataFrame = lineValidator.validate(dataFrame);
        }
        dataFrame.show();
        return dataFrame;
    }
}
