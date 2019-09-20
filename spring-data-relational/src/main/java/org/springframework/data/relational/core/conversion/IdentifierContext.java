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
package org.springframework.data.relational.core.conversion;

import java.util.function.Function;

import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.domain.Identifier;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * @author Jens Schauder
 */
interface IdentifierContext {

	static IdentifierContext of(
			Function<MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty>, Object> baseId) {
		return new RootIdentifierContext(baseId);
	}

	IdentifierContext withIdPath(PersistentPropertyPath<RelationalPersistentProperty> path,
			Function<MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty>, Object> id);

	IdentifierContext withQualifier(PersistentPropertyPath<RelationalPersistentProperty> path, Object value);

	Identifier toIdentifier(MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty> context,
			PersistentPropertyPath<RelationalPersistentProperty> path);

	JdbcIdentifierBuilder identityBuilder(
			MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty> context,
			PersistentPropertyPath<RelationalPersistentProperty> path);

	IdentifierContext withQualifier(PersistentPropertyPath<RelationalPersistentProperty> path,
			@Nullable Object some_map_key,
			@Nullable Function<MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty>, Object> id);

	class RootIdentifierContext implements IdentifierContext {
		private final Function<MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty>, Object> id;

		RootIdentifierContext(
				Function<MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty>, Object> id) {
			this.id = id;
		}

		@Override
		public IdentifierContext withIdPath(PersistentPropertyPath<RelationalPersistentProperty> path,
				Function<MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty>, Object> id) {
			return this;
		}

		public IdentifierContext withQualifier(PersistentPropertyPath<RelationalPersistentProperty> path, Object value) {

			return new NodeIdentifierContext(this, path, value);
		}

		@Override
		public Identifier toIdentifier(MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty> context,
				PersistentPropertyPath<RelationalPersistentProperty> path) {
			return identityBuilder(context, path).build();

		}

		public JdbcIdentifierBuilder identityBuilder(
				MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty> context,
				PersistentPropertyPath<RelationalPersistentProperty> path) {

			if (path == null) {
				return JdbcIdentifierBuilder.empty();
			}

			Object value = id.apply(context);
			Assert.notNull(value, "An id must not be null when requesting an identifier");

			return JdbcIdentifierBuilder //
					.forBackReferences(new PersistentPropertyPathExtension(context, path), value);
		}

		@Override
		public IdentifierContext withQualifier(PersistentPropertyPath<RelationalPersistentProperty> path, Object value,
				Function<MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty>, Object> id) {
			return new NodeIdentifierContext(this, path, value, id);
		}
	}

	class NodeIdentifierContext implements IdentifierContext {
		private final IdentifierContext parent;
		private final PersistentPropertyPath<RelationalPersistentProperty> path;
		private final Object value;
		private final Function<MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty>, Object> id;

		public NodeIdentifierContext(IdentifierContext parent, PersistentPropertyPath<RelationalPersistentProperty> path,
				Object value) {
			this(parent, path, value, null);
		}

		public NodeIdentifierContext(IdentifierContext parent, PersistentPropertyPath<RelationalPersistentProperty> path,
				Object value,
				Function<MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty>, Object> id) {

			this.parent = parent;
			this.path = path;
			this.value = value;
			this.id = id;
		}

		@Override
		public IdentifierContext withIdPath(PersistentPropertyPath<RelationalPersistentProperty> path,
				Function<MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty>, Object> id) {
			return this;
		}

		@Override
		public IdentifierContext withQualifier(PersistentPropertyPath<RelationalPersistentProperty> path, Object value) {
			return new NodeIdentifierContext(this, path, value);
		}

		@Override
		public Identifier toIdentifier(MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty> context,
				PersistentPropertyPath<RelationalPersistentProperty> path) {

			return identityBuilder(context, path).build();

		}

		public JdbcIdentifierBuilder identityBuilder(
				MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty> context,
				PersistentPropertyPath<RelationalPersistentProperty> path) {

			if (path.getLength() > this.path.getLength() && id != null) {
				Object value = id.apply(context);
				Assert.notNull(value, "An id must not be null when requesting an identifier");
				return JdbcIdentifierBuilder.forBackReferences(new PersistentPropertyPathExtension(context, path), value);
			}

			JdbcIdentifierBuilder builder = parent.identityBuilder(context, path);
			if (value == null) {
				return builder;
			}
			return builder.withQualifier(new PersistentPropertyPathExtension(context, this.path), value);
		}

		@Override
		public IdentifierContext withQualifier(PersistentPropertyPath<RelationalPersistentProperty> path, Object value,
				Function<MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty>, Object> id) {
			return new NodeIdentifierContext(this, path, value, id);
		}
	}

}
