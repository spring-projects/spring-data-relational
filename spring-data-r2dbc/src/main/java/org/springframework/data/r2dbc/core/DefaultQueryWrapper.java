package org.springframework.data.r2dbc.core;

import org.springframework.data.relational.core.query.Query;

public class DefaultQueryWrapper implements QueryWrapper{
    @Override
    public Query wrapper(Query query, Class<?> domainType) {
        return query;
    }
}
