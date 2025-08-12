package org.springframework.data.r2dbc.core;

import org.springframework.data.relational.core.query.Query;

public interface QueryWrapper {

    Query wrapper(Query query, Class<?> domainType);
}
