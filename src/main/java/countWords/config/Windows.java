package countWords.config;

import org.springframework.context.annotation.Conditional;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Created by Evegeny on 11/06/2016.
 */
@Retention(RUNTIME)
@Conditional(WindowsDetector.class)
public @interface Windows {
    boolean value() default true;
}












