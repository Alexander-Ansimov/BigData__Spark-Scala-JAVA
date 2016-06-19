package football;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by Evegeny on 08/06/2016.
 */
@Component
public class TeamsMapper  implements Mapper{
    public Map<Object, List<String>> teams;

    @Override
    @Autowired
    @Qualifier("teams")
    public void set(Properties properties) {
        teams = MapperUtils.readProperties(properties);
    }

    @Override
    public Map<Object, List<String>> get() {
        return teams;
    }
}
