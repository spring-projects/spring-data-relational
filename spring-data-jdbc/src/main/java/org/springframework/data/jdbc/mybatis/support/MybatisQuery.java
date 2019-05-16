package org.springframework.data.jdbc.mybatis.support;

import org.springframework.data.annotation.QueryAnnotation;

import java.lang.annotation.*;

/**
 * @author Songling.Dong
 * Created on 2019/5/8.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@QueryAnnotation
@Documented
public @interface MybatisQuery {
}
