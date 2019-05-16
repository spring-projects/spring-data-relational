package org.springframework.data.jdbc.mybatis.support;

import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.lang.Nullable;

/**
 * @author Songling.Dong
 * Created on 2019/5/15.
 */
public interface MybatisContext {


    @Nullable
    default SqlSessionTemplate getSqlSessionTemplate() {
        return null;
    }

    MybatisContext EMPTY = new MybatisContext() {


    };
}
