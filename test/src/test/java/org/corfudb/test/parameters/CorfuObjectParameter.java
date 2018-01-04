package org.corfudb.test.parameters;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.corfudb.runtime.view.ObjectOpenOptions;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface CorfuObjectParameter {
    String stream() default "test";
    String name() default "";
    ObjectOpenOptions[] options() default {};
}
