package org.springframework.data.jdbc.testing;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.PreparedStatementCallback;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.jdbc.support.rowset.SqlRowSet;

/**
 * {@link NamedParameterJdbcOperations} implementation for tests, that is capable to audit the executed queries. The
 * actual execution is delegated to {@link #delegate}.
 *
 * @author Mikhail Polivakha
 */
public class AuditableNamedParameterJdbcTemplate implements NamedParameterJdbcOperations {

	private final NamedParameterJdbcOperations delegate;
	private final Queue<TestQuery> statementsQueue;

	public AuditableNamedParameterJdbcTemplate(NamedParameterJdbcOperations delegate) {
		this.delegate = delegate;
		this.statementsQueue = new LinkedList<>();
	}

	public int statementsQueueSize() {
		return statementsQueue.size();
	}

	public void clearRecordedStatements() {
		statementsQueue.clear();
	}

	public void assertSequenceOfStatements(QueryType... types) {
		if (types.length != statementsQueue.size()) {
			Assertions.fail(
					"Expected sequence of statements %s has different size then actually executed statements: %s".formatted(
							Arrays.toString(types), statementsQueue));
		}

		int index = 0;

		for (TestQuery testQuery : statementsQueue) {
			QueryType type = types[index++];

			Assertions.assertThat(type).isEqualTo(testQuery.queryType).describedAs(
					"Expected query at position %d to have type '%s', but actual query was '%s'".formatted(index - 1, type,
							testQuery));
		}
	}

	@Override
	public JdbcOperations getJdbcOperations() {
		return delegate.getJdbcOperations();
	}

	@NotNull
	@Override
	public <T> T execute(String sql, SqlParameterSource paramSource, PreparedStatementCallback<T> action)
			throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.execute(sql, paramSource, action);
	}

	@Override
	public <T> T execute(String sql, Map<String, ?> paramMap, PreparedStatementCallback<T> action)
			throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.execute(sql, paramMap, action);
	}

	@Override
	public <T> T execute(String sql, PreparedStatementCallback<T> action) throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.execute(sql, action);
	}

	@Override
	public <T> T query(String sql, SqlParameterSource paramSource, ResultSetExtractor<T> rse) throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.query(sql, paramSource, rse);
	}

	@Override
	public <T> T query(String sql, Map<String, ?> paramMap, ResultSetExtractor<T> rse) throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.query(sql, paramMap, rse);
	}

	@Override
	public <T> T query(String sql, ResultSetExtractor<T> rse) throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.query(sql, rse);
	}

	@Override
	public void query(String sql, SqlParameterSource paramSource, RowCallbackHandler rch) throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		delegate.query(sql, paramSource, rch);
	}

	@Override
	public void query(String sql, Map<String, ?> paramMap, RowCallbackHandler rch) throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		delegate.query(sql, paramMap, rch);
	}

	@Override
	public void query(String sql, RowCallbackHandler rch) throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		delegate.query(sql, rch);
	}

	@Override
	public <T> List<T> query(String sql, SqlParameterSource paramSource, RowMapper<T> rowMapper)
			throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.query(sql, paramSource, rowMapper);
	}

	@Override
	public <T> List<T> query(String sql, Map<String, ?> paramMap, RowMapper<T> rowMapper) throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.query(sql, paramMap, rowMapper);
	}

	@Override
	public <T> List<T> query(String sql, RowMapper<T> rowMapper) throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.query(sql, rowMapper);
	}

	@Override
	public <T> Stream<T> queryForStream(String sql, SqlParameterSource paramSource, RowMapper<T> rowMapper)
			throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.queryForStream(sql, paramSource, rowMapper);
	}

	@Override
	public <T> Stream<T> queryForStream(String sql, Map<String, ?> paramMap, RowMapper<T> rowMapper)
			throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.queryForStream(sql, paramMap, rowMapper);
	}

	@Override
	public <T> T queryForObject(String sql, SqlParameterSource paramSource, RowMapper<T> rowMapper)
			throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.queryForObject(sql, paramSource, rowMapper);
	}

	@Override
	public <T> T queryForObject(String sql, Map<String, ?> paramMap, RowMapper<T> rowMapper) throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.queryForObject(sql, paramMap, rowMapper);
	}

	@Override
	public <T> T queryForObject(String sql, SqlParameterSource paramSource, Class<T> requiredType)
			throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.queryForObject(sql, paramSource, requiredType);
	}

	@Override
	public <T> T queryForObject(String sql, Map<String, ?> paramMap, Class<T> requiredType) throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.queryForObject(sql, paramMap, requiredType);
	}

	@Override
	public Map<String, Object> queryForMap(String sql, SqlParameterSource paramSource) throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.queryForMap(sql, paramSource);
	}

	@Override
	public Map<String, Object> queryForMap(String sql, Map<String, ?> paramMap) throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.queryForMap(sql, paramMap);
	}

	@Override
	public <T> List<T> queryForList(String sql, SqlParameterSource paramSource, Class<T> elementType)
			throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.queryForList(sql, paramSource, elementType);
	}

	@Override
	public <T> List<T> queryForList(String sql, Map<String, ?> paramMap, Class<T> elementType)
			throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.queryForList(sql, paramMap, elementType);
	}

	@Override
	public List<Map<String, Object>> queryForList(String sql, SqlParameterSource paramSource) throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.queryForList(sql, paramSource);
	}

	@Override
	public List<Map<String, Object>> queryForList(String sql, Map<String, ?> paramMap) throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.queryForList(sql, paramMap);
	}

	@Override
	public SqlRowSet queryForRowSet(String sql, SqlParameterSource paramSource) throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.queryForRowSet(sql, paramSource);
	}

	@Override
	public SqlRowSet queryForRowSet(String sql, Map<String, ?> paramMap) throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.queryForRowSet(sql, paramMap);
	}

	@Override
	public int update(String sql, SqlParameterSource paramSource) throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.update(sql, paramSource);
	}

	@Override
	public int update(String sql, Map<String, ?> paramMap) throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.update(sql, paramMap);
	}

	@Override
	public int update(String sql, SqlParameterSource paramSource, KeyHolder generatedKeyHolder)
			throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.update(sql, paramSource, generatedKeyHolder);
	}

	@Override
	public int update(String sql, SqlParameterSource paramSource, KeyHolder generatedKeyHolder, String[] keyColumnNames)
			throws DataAccessException {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.update(sql, paramSource, generatedKeyHolder, keyColumnNames);
	}

	@Override
	public int[] batchUpdate(String sql, SqlParameterSource[] batchArgs) {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.batchUpdate(sql, batchArgs);
	}

	@Override
	public int[] batchUpdate(String sql, Map<String, ?>[] batchValues) {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.batchUpdate(sql, batchValues);
	}

	@Override
	public int[] batchUpdate(String sql, SqlParameterSource[] batchArgs, KeyHolder generatedKeyHolder) {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.batchUpdate(sql, batchArgs, generatedKeyHolder);
	}

	@Override
	public int[] batchUpdate(String sql, SqlParameterSource[] batchArgs, KeyHolder generatedKeyHolder,
			String[] keyColumnNames) {
		statementsQueue.add(TestQuery.from(sql));
		return delegate.batchUpdate(sql, batchArgs, generatedKeyHolder, keyColumnNames);
	}

	public record TestQuery(QueryType queryType, String query) {

		static TestQuery from(String query) {
			return new TestQuery(QueryType.fromQuery(query), query);
		}
	}

	public enum QueryType {

		INSERT, UPDATE, SELECT, DELETE, OTHER;

		static QueryType fromQuery(String query) {
			for (QueryType value : values()) {
				if (query.trim().startsWith(value.name())) {
					return value;
				}
			}

			return OTHER;
		}
	}
}
