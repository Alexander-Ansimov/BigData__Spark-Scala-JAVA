package football;

import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by Evegeny on 11/06/2016.
 */
@Service
public class FootballService {
    @Autowired
    private DataFrameCreator creator;

    @Autowired
    private ValidatorAggregator validatorAggregator;

    public void doWork() {
        DataFrame dataFrame = creator.create("data/rawData.txt");
        DataFrame validate = validatorAggregator.validate(dataFrame);
        validate.persist();
        validate.show();
    }












}
