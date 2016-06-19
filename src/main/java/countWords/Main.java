package countWords;

import countWords.config.RootConfig;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Created by Evegeny on 11/06/2016.
 */
public class Main {
    public static void main(String[] args) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(RootConfig.class);

    }
}
