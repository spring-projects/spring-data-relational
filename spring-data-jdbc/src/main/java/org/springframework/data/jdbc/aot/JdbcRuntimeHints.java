/*
 * Copyright 2022-2024 the original author or authors.
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
package org.springframework.data.jdbc.aot;

import java.util.Arrays;

import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.aot.hint.TypeReference;
import org.springframework.data.jdbc.core.dialect.JdbcPostgresDialect;
import org.springframework.data.jdbc.repository.support.SimpleJdbcRepository;
import org.springframework.data.relational.auditing.RelationalAuditingCallback;
import org.springframework.data.relational.core.mapping.event.AfterConvertCallback;
import org.springframework.data.relational.core.mapping.event.AfterDeleteCallback;
import org.springframework.data.relational.core.mapping.event.AfterSaveCallback;
import org.springframework.data.relational.core.mapping.event.BeforeConvertCallback;
import org.springframework.data.relational.core.mapping.event.BeforeDeleteCallback;
import org.springframework.data.relational.core.mapping.event.BeforeSaveCallback;
import org.springframework.lang.Nullable;

/**
 * {@link RuntimeHintsRegistrar} for JDBC.
 *
 * @author Christoph Strobl
 * @since 3.0
 */
class JdbcRuntimeHints implements RuntimeHintsRegistrar {

	@Override
	public void registerHints(RuntimeHints hints, @Nullable ClassLoader classLoader) {

		hints.reflection().registerTypes(
				Arrays.asList(TypeReference.of(SimpleJdbcRepository.class), TypeReference.of(AfterConvertCallback.class),
						TypeReference.of(AfterDeleteCallback.class), TypeReference.of(AfterSaveCallback.class),
						TypeReference.of(BeforeConvertCallback.class), TypeReference.of(BeforeDeleteCallback.class),
						TypeReference.of(BeforeSaveCallback.class), TypeReference.of(RelationalAuditingCallback.class)),
				builder -> builder.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
						MemberCategory.INVOKE_PUBLIC_METHODS));

		hints.proxies().registerJdkProxy(TypeReference.of("org.springframework.data.jdbc.core.convert.RelationResolver"),
				TypeReference.of("org.springframework.aop.SpringProxy"),
				TypeReference.of("org.springframework.aop.framework.Advised"),
				TypeReference.of("org.springframework.core.DecoratingProxy"));

		hints.reflection().registerType(TypeReference.of("org.postgresql.jdbc.TypeInfoCache"),
				MemberCategory.PUBLIC_CLASSES);

		for (Class<?> simpleType : JdbcPostgresDialect.INSTANCE.simpleTypes()) {
			hints.reflection().registerType(TypeReference.of(simpleType), MemberCategory.PUBLIC_CLASSES);
		}
	}
}
