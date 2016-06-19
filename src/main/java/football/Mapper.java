package football;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Evegeny on 08/06/2016.
 */
public interface Mapper {
    void set(Properties properties);

    Map<Object, List<String>> get();
}
