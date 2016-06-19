package football;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Evegeny on 08/06/2016.
 */
@Component
public class CodesMapper implements Mapper {

    private Map<Object, List<String>> codes;

    @Override
    @Autowired
    @Qualifier("codes")
    public void set(Properties properties) {
        codes = MapperUtils.readProperties(properties);
    }

    @Override
    public Map<Object, List<String>> get() {
        return codes;
    }
}
