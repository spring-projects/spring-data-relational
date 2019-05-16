package org.springframework.data.jdbc.mybatis.support;

import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryMethod;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author Songling.Dong
 * Created on 2019/5/9.
 */
public class MybatisQueryMethod extends QueryMethod {

    private final Method method;
    private final Object mapper;

    /**
     * Creates a new {@link QueryMethod} from the given parameters. Looks up the correct query to use for following
     * invocations of the method given.
     *
     * @param method   must not be {@literal null}.
     * @param metadata must not be {@literal null}.
     * @param factory  must not be {@literal null}.
     */
    public MybatisQueryMethod(Method method, RepositoryMetadata metadata, ProjectionFactory factory, Object mapper) {
        super(method, metadata, factory);
        this.method = method;
        this.mapper = mapper;
    }



    Object invoke(Object[] args) throws InvocationTargetException, IllegalAccessException {
        return method.invoke(mapper, args);
    }

    @Override
    public boolean isModifyingQuery() {
        return AnnotationUtils.findAnnotation(method, Modifying.class) != null;
    }
}
