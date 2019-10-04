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
 * An {@code IdentifierContext} contains all the information of and {@link Identifier} but isn't bound to a specific
 * path yet. An {@code Identifier} is bound to to a certain path because it contains backreferences that might result in
 * different column names depending on where they are used. Consider and aggregate as the following:
 * {@literal A -> B -> C} with only {@literal A} having an Id attribute. The table for {@literal B} will have a
 * backreference to {@literal A} as (part of) its primary key and so will the table for {@literal C}. But the column
 * names may differ. So an {@code IdentifierContext} will have the information about the back reference, but only
 * {@code Identifier} will have the actual column names.
 *
 * @author Jens Schauder
 * @see Identifier
 * @since 2.0
 */
interface IdentifierContext {

	/**
	 * Creates the simplest possible {@code IdentifierContext} holding just the id of a root entity (with an empty path).
	 *
	 * @param baseId a function which will provide the id, possibly after it was generated which might happen after
	 *          instantiation of the {@code IdentifierContext}. Must not be {@literal null}.
	 * @return Guaranteed to be not {@literal null}.
	 */
	static IdentifierContext of(
			Function<MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty>, Object> baseId) {

		Assert.notNull(baseId, "BaseId must not be null.");

		return new RootIdentifierContext(baseId);
	}

	/**
	 * Creates a new context based on this context plus an additional qualifier, i.e. a pair of a path and a value.
	 *
	 * @param path Must not be {@literal null}.
	 * @param value Must not be {@literal null}.
	 * @return Guaranteed to be not {@literal null}.
	 */
	IdentifierContext withQualifier(PersistentPropertyPath<RelationalPersistentProperty> path, Object value);

	/**
	 * Builds an {@link org.springframework.data.relational.domain.Identifier} from this context.
	 *
	 * @param context The context to be used for resolving backreferences. Must not be {@literal null}.
	 * @param path The path relative to which backreferences get resolved. Must not be {@literal null}.
	 * @return Guaranteed to be not {@literal null}.
	 */
	Identifier toIdentifier(MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty> context,
			PersistentPropertyPath<RelationalPersistentProperty> path);

	JdbcIdentifierBuilder identityBuilder(
			MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty> context,
			PersistentPropertyPath<RelationalPersistentProperty> path);

	IdentifierContext withQualifier(PersistentPropertyPath<RelationalPersistentProperty> path,
			@Nullable Object some_map_key,
			@Nullable Function<MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty>, Object> id);

	/**
	 * An {@link IdentifierContext} representing a simple id of a root element.
	 */
	class RootIdentifierContext implements IdentifierContext {

		private final Function<MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty>, Object> id;

		RootIdentifierContext(
				Function<MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty>, Object> id) {
			this.id = id;
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

			Object value = id.apply(context);
			Assert.notNull(value, "An id must not be null when requesting an identifier");

			return JdbcIdentifierBuilder //
					.forBackReferences(new PersistentPropertyPathExtension(context, path), value);
		}

		@Override
		public IdentifierContext withQualifier(PersistentPropertyPath<RelationalPersistentProperty> path,
				@Nullable Object qualifier,
				@Nullable Function<MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty>, Object> id) {
			return new NodeIdentifierContext(this, path, qualifier, id);
		}
	}

	/**
	 * An {@link IdentifierContext} consisting of a qualifier or an id or possibly both.
	 */
	class NodeIdentifierContext implements IdentifierContext {

		private final IdentifierContext parent;
		private final PersistentPropertyPath<RelationalPersistentProperty> path;
		private final Object qualifier;
		@Nullable private final Function<MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty>, Object> id;

		NodeIdentifierContext(IdentifierContext parent, PersistentPropertyPath<RelationalPersistentProperty> path,
				Object qualifier) {
			this(parent, path, qualifier, null);
		}

		NodeIdentifierContext(IdentifierContext parent, PersistentPropertyPath<RelationalPersistentProperty> path,
				@Nullable Object qualifier,
				@Nullable Function<MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty>, Object> id) {

			this.parent = parent;
			this.path = path;
			this.qualifier = qualifier;
			this.id = id;
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
			if (qualifier == null) {
				return builder;
			}
			return builder.withQualifier(new PersistentPropertyPathExtension(context, this.path), qualifier);
		}

		@Override
		public IdentifierContext withQualifier(PersistentPropertyPath<RelationalPersistentProperty> path,
				@Nullable Object qualifier,
				@Nullable Function<MappingContext<RelationalPersistentEntity<?>, RelationalPersistentProperty>, Object> id) {
			return new NodeIdentifierContext(this, path, qualifier, id);
		}
	}
}
