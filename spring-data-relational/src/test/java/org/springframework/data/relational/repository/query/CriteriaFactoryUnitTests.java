/*
 * Copyright 2020-2024 the original author or authors.
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
package org.springframework.data.relational.repository.query;

import static org.assertj.core.api.Assertions.*;


import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.parser.Part;

/**
 * Unit tests for {@link CriteriaFactory}.
 *
 * @author Mark Paluch
 */
public class CriteriaFactoryUnitTests {

	@Test // DATAJDBC-539
	void shouldConsiderIterableValuesInInOperator() {

		QueryMethod queryMethod = getQueryMethod("findAllByNameIn", List.class);
		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod, Arrays.asList("foo", "bar"));
		ParameterMetadataProvider parameterMetadata = new ParameterMetadataProvider(accessor);
		CriteriaFactory criteriaFactory = new CriteriaFactory(parameterMetadata);

		Part part = new Part("NameIn", User.class);

		Criteria criteria = criteriaFactory.createCriteria(part);

		assertThat(criteria.getValue()).isEqualTo(Arrays.asList("foo", "bar"));
	}

	@Test // DATAJDBC-539
	void shouldConsiderArrayValuesInInOperator() {

		QueryMethod queryMethod = getQueryMethod("findAllByNameIn", String[].class);

		RelationalParametersParameterAccessor accessor = getAccessor(queryMethod,
				new Object[] { new String[] { "foo", "bar" } });
		ParameterMetadataProvider parameterMetadata = new ParameterMetadataProvider(accessor);
		CriteriaFactory criteriaFactory = new CriteriaFactory(parameterMetadata);

		Part part = new Part("NameIn", User.class);

		Criteria criteria = criteriaFactory.createCriteria(part);

		assertThat(criteria.getValue()).isEqualTo(Arrays.asList("foo", "bar"));
	}

	private QueryMethod getQueryMethod(String methodName, Class<?>... parameterTypes) {

		Method method = null;
		try {
			method = UserRepository.class.getMethod(methodName, parameterTypes);
		} catch (NoSuchMethodException e) {
			throw new RuntimeException(e);
		}
		return new QueryMethod(method, new DefaultRepositoryMetadata(UserRepository.class),
				new SpelAwareProxyProjectionFactory());
	}

	private RelationalParametersParameterAccessor getAccessor(QueryMethod queryMethod, Object... values) {
		return new RelationalParametersParameterAccessor(queryMethod, values);
	}

	interface UserRepository extends Repository<User, Long> {

		User findAllByNameIn(List<String> names);

		User findAllByNameIn(String[] names);
	}

	static class User {

		String name;
	}
}
