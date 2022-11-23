package utils;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 加在实体类上，表示当前类支持JDBCUtils
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Support {
    /**
     * 指定实体类所对应的表名，如不写则默认是类字母小写
     * @return
     */
    String value()default "";
}
