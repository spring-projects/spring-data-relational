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

import org.junit.jupiter.api.Test;

import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.relational.core.dialect.Escaper;
import org.springframework.data.relational.core.query.ValueFunction;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.DefaultRepositoryMetadata;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * Unit tests for {@link ParameterMetadataProvider}.
 *
 * @author Mark Paluch
 */
public class ParameterMetadataProviderUnitTests {

	@Test // DATAJDBC-514
	public void shouldCreateValueFunctionForContains() throws Exception {

		ParameterMetadata metadata = getParameterMetadata("findByNameContains", "hell%o");

		assertThat(metadata.getValue()).isInstanceOf(ValueFunction.class);
		ValueFunction<Object> function = (ValueFunction<Object>) metadata.getValue();
		assertThat(function.apply(Escaper.DEFAULT)).isEqualTo("%hell\\%o%");
	}

	@Test // DATAJDBC-514
	public void shouldCreateValueFunctionForStartingWith() throws Exception {

		ParameterMetadata metadata = getParameterMetadata("findByNameStartingWith", "hell%o");

		assertThat(metadata.getValue()).isInstanceOf(ValueFunction.class);
		ValueFunction<Object> function = (ValueFunction<Object>) metadata.getValue();
		assertThat(function.apply(Escaper.DEFAULT)).isEqualTo("hell\\%o%");
	}

	@Test // DATAJDBC-514
	public void shouldCreateValue() throws Exception {

		ParameterMetadata metadata = getParameterMetadata("findByName", "hell%o");

		assertThat(metadata.getValue()).isEqualTo("hell%o");
	}

	private ParameterMetadata getParameterMetadata(String methodName, Object value) throws Exception {

		Method method = UserRepository.class.getMethod(methodName, String.class);
		ParameterMetadataProvider provider = new ParameterMetadataProvider(new RelationalParametersParameterAccessor(
				new RelationalQueryMethod(method, new DefaultRepositoryMetadata(UserRepository.class),
						new SpelAwareProxyProjectionFactory()),
				new Object[] { value }));

		PartTree tree = new PartTree(methodName, User.class);

		return provider.next(tree.getParts().iterator().next());
	}

	static class RelationalQueryMethod extends QueryMethod {

		public RelationalQueryMethod(Method method, RepositoryMetadata metadata, ProjectionFactory factory) {
			super(method, metadata, factory);
		}
	}

	interface UserRepository extends Repository<User, String> {

		String findByNameStartingWith(String prefix);

		String findByNameContains(String substring);

		String findByName(String substring);
	}

	static class User {
		String name;
	}
}
