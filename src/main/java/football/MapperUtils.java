package football;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Created by Evegeny on 08/06/2016.
 */
public class MapperUtils {
    public static Map<Object, List<String>> readProperties(Properties properties) {
       return properties.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> {
            String teams1 = (String) entry.getValue();
            return Arrays.asList(teams1.split(","));
        }));
    }
}
