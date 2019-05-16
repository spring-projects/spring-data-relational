package org.springframework.data.jdbc.mybatis.support;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.event.AfterLoadEvent;
import org.springframework.data.relational.core.mapping.event.Identifier;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.lang.Nullable;

import java.lang.reflect.InvocationTargetException;

/**
 * @author Songling.Dong
 * Created on 2019/5/9.
 */
public class MybatisRepositoryQuery implements RepositoryQuery {


    private final ApplicationEventPublisher publisher;
    private final RelationalMappingContext context;
    private final MybatisQueryMethod mybatisQueryMethod;

    public MybatisRepositoryQuery(ApplicationEventPublisher publisher, RelationalMappingContext context, MybatisQueryMethod mybatisQueryMethod) {
        this.publisher = publisher;
        this.context = context;
        this.mybatisQueryMethod = mybatisQueryMethod;
    }

    @Override
    public Object execute(Object[] parameters) {
        try {
            Object retVal = mybatisQueryMethod.invoke(parameters);
            if (!mybatisQueryMethod.isModifyingQuery()) {
                if (mybatisQueryMethod.isCollectionQuery() || mybatisQueryMethod.isStreamQuery()) {
                    publishAfterLoad((Iterable<?>) retVal);
                } else {
                    publishAfterLoad(retVal);
                }
            }
            return retVal;
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e.getTargetException());
        } catch (IllegalAccessException e) {
            throw new RuntimeException("invoke from mybatis mapper failed", e);
        }
    }

    @Override
    public MybatisQueryMethod getQueryMethod() {
        return mybatisQueryMethod;
    }


    private <T> void publishAfterLoad(Iterable<T> all) {

        for (T e : all) {
            publishAfterLoad(e);
        }
    }

    private <T> void publishAfterLoad(@Nullable T entity) {
        //duplicate code.
        if (entity != null && context.hasPersistentEntityFor(entity.getClass())) {

            RelationalPersistentEntity<?> e = context.getRequiredPersistentEntity(entity.getClass());
            Object identifier = e.getIdentifierAccessor(entity)
                    .getIdentifier();

            if (identifier != null) {
                publisher.publishEvent(new AfterLoadEvent(Identifier.of(identifier), entity));
            }
        }

    }

}
