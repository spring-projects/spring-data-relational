/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.r2dbc.repository.support;

import static org.assertj.core.api.Assertions.*;

import io.r2dbc.spi.ConnectionFactory;
import reactor.test.StepVerifier;

import java.util.Map;

import javax.sql.DataSource;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessException;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.testing.H2TestSupport;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Integration tests for {@link SimpleR2dbcRepository} against H2.
 *
 * @author Mark Paluch
 */
@RunWith(SpringRunner.class)
@ContextConfiguration
public class H2SimpleR2dbcRepositoryIntegrationTests extends AbstractSimpleR2dbcRepositoryIntegrationTests {

	@Configuration
	static class IntegrationTestConfiguration extends AbstractR2dbcConfiguration {

		@Override
		public ConnectionFactory connectionFactory() {
			return H2TestSupport.createConnectionFactory();
		}
	}

	@Override
	protected DataSource createDataSource() {
		return H2TestSupport.createDataSource();
	}

	@Override
	protected String getCreateTableStatement() {
		return H2TestSupport.CREATE_TABLE_LEGOSET_WITH_ID_GENERATION;
	}

	@Test // gh-90
	public void shouldInsertNewObjectWithGivenId() {

		try {
			this.jdbc.execute("DROP TABLE legoset");
		} catch (DataAccessException e) {}

		this.jdbc.execute(H2TestSupport.CREATE_TABLE_LEGOSET);

		AlwaysNewLegoSet legoSet = new AlwaysNewLegoSet(9999, "SCHAUFELRADBAGGER", 12);

		repository.save(legoSet) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getId()).isEqualTo(9999);
				}).verifyComplete();

		Map<String, Object> map = jdbc.queryForMap("SELECT * FROM legoset");
		assertThat(map).containsEntry("name", "SCHAUFELRADBAGGER").containsEntry("manual", 12).containsKey("id");
	}
}
