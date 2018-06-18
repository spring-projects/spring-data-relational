/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.jdbc.core.r2dbc;

import org.reactivestreams.Publisher;
import org.springframework.dao.DataAccessException;
import org.springframework.util.Assert;

import io.r2dbc.spi.Result;

/**
 * Adapter implementation of the {@link ResultExtractor} interface that delegates to a {@link RowMapper} which is
 * supposed to create an object for each row. Each object is added to the results List of this {@link ResultExtractor}.
 * <p>
 * Useful for the typical case of one object per row in the database table. The number of entries in the results list
 * will match the number of rows.
 * <p>
 * Note that a RowMapper object is typically stateless and thus reusable; just the RowMapperResultSetExtractor adapter
 * is stateful.
 * <p>
 * A usage example with R2dbcTemplate:
 *
 * <pre class="code">
 * R2dbcTemplate r2dbcTemplate = new R2dbcTemplate(connectionFactory); // reusable object
 * RowMapper rowMapper = new UserRowMapper(); // reusable object
 *
 * Flux allUsers = r2dbcTemplate.query("select * from user", new RowMapperResultSetExtractor(rowMapper, 10));
 *
 * Flux<User></User> user = r2dbcTemplate.queryForObject("select * from user where id=?", new Object[] { id },
 * 		new RowMapperResultSetExtractor(rowMapper, 1));
 * </pre>
 *
 * @author Mark Paluch
 * @see RowMapper
 * @see R2dbcTemplate
 */
public class RowMapperResultExtractor<T> implements ResultExtractor<T> {

	private final RowMapper<T> rowMapper;

	/**
	 * Create a new RowMapperResultSetExtractor.
	 *
	 * @param rowMapper the RowMapper which creates an object for each row
	 */
	public RowMapperResultExtractor(RowMapper<T> rowMapper) {
		Assert.notNull(rowMapper, "RowMapper is required");
		this.rowMapper = rowMapper;
	}

	@Override
	public Publisher<T> extractData(Result rs) throws DataAccessException {
		return rs.map(this.rowMapper);
	}
}
