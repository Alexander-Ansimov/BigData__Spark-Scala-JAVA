package football;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Evegeny on 08/06/2016.
 */
@Component
public class RulesMapper implements Mapper {
    public Map<Object, List<String>> rules;

    @Override
    @Autowired
    @Qualifier("rules")
    public void set(Properties properties) {
        rules = MapperUtils.readProperties(properties);
    }

    @Override
    public Map<Object, List<String>> get() {
        return rules;
    }


}
