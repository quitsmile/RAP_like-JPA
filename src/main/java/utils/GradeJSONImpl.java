package utils;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * 将传入的map转换成指定的类的对象
 * {@code @Author} R
 */
public class GradeJSONImpl  {


    public  static<T> T getGrande(Map<String, Object> map,Class<T> o) {
       Field [] f =o.getDeclaredFields();
        T g;
        try {
            g=o.newInstance();

            for (Field e:f
                 ) {

                if (map.get(e.getName())!=null){

                    e.setAccessible(true);
                    e.set(g,map.get(e.getName()));
                }
            }

        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        return g;

    }

}
