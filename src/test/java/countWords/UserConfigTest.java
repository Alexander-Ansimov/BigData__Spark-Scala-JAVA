package countWords;

import countWords.config.RootConfig;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.*;

/**
 * Created by Evegeny on 11/06/2016.
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = RootConfig.class)
public class UserConfigTest {
    @Autowired
    UserConfig userConfig;

    @Test
    public void testUserConfigContent() throws Exception {
        System.out.println(userConfig.garbage);

    }
}












