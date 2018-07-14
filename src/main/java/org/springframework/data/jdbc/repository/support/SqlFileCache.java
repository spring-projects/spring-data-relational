/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.jdbc.repository.support;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * a cache for sql files
 *
 * @author Toshiaki Maki
 */
class SqlFileCache {
    final ConcurrentMap<String, String> sqlFileMap = new ConcurrentHashMap<>();

    public Optional<String> getSql(String resourcePath) {
        String sqlFile = sqlFileMap.get(resourcePath);
        return Optional.ofNullable(sqlFile);
    }

    public String putSqlIfAbsent(String resourcePath, String sql) {
        String current = this.sqlFileMap.putIfAbsent(resourcePath, sql);
        return current != null ? current : sql;
    }
}
