package football;

import org.fluttercode.datafactory.impl.DataFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by Evegeny on 10/06/2016.
 */
@Service
public class FootballDataGenerator {
    @Autowired
    private CodesMapper codesMapper;
    @Autowired
    private TeamsMapper teamsMapper;
    @Autowired
    private RulesMapper rulesMapper;

    private Random random = new Random();

    public void generate() {
        //  3       Denys BOYKO;  Denys BOYKO;  17:00       Dinamo          16:00
        StringBuilder sb = new StringBuilder();
        DataFactory dataFactory = new DataFactory();
        sb.append("code=").append(random.nextInt(13)).append(";");
        sb.append("from=").append(getRandomName()).append(";");
        sb.append("to=").append(getRandomName()).append(";");
        sb.append("eventTime=").append(getRandomTime()).append(";");
        sb.append("stadion=").append(dataFactory.getRandomWord()).append(";");
        sb.append("startTime=").append(getRandomTime()).append(";");
        System.out.println(sb.toString());

    }

    private String getRandomTime() {
        int hh = random.nextInt(23);
        int mm = random.nextInt(59);
        return hh+":"+mm;
    }

    @PostConstruct
    private String getRandomName() {

        Map<Object, List<String>> map = teamsMapper.get();

        List<String> list = map.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        return list.get(random.nextInt(list.size()));
    }
}
