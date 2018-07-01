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
