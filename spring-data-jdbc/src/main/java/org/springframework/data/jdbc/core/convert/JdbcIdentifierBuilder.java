/*
 * Copyright 2019-2025 the original author or authors.
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
package org.springframework.data.jdbc.core.convert;

import java.util.function.Function;

import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.util.Assert;

/**
 * Builder for {@link Identifier}. Mainly for internal use within the framework
 *
 * @author Jens Schauder
 * @since 1.1
 */
public class JdbcIdentifierBuilder {

	private Identifier identifier;

	private JdbcIdentifierBuilder(Identifier identifier) {
		this.identifier = identifier;
	}

	public static JdbcIdentifierBuilder empty() {
		return new JdbcIdentifierBuilder(Identifier.empty());
	}

	/**
	 * Creates ParentKeys with backreference for the given path and value of the parents id.
	 */
	public static JdbcIdentifierBuilder forBackReferences(JdbcConverter converter, AggregatePath path,
			Function<AggregatePath, Object> valueProvider) {

		return new JdbcIdentifierBuilder(forBackReference(converter, path, Identifier.empty(), valueProvider));
	}

	/**
	 * @param converter used for determining the column types to be used for different properties. Must not be
	 *          {@literal null}.
	 * @param path the path for which needs to back reference an id. Must not be {@literal null}.
	 * @param defaultIdentifier Identifier to be used as a default when no backreference can be constructed. Must not be
	 *          {@literal null}.
	 * @param valueProvider provides values for the {@link Identifier} based on an {@link AggregatePath}. Must not be
	 *          {@literal null}.
	 * @return Guaranteed not to be {@literal null}.
	 */
	public static Identifier forBackReference(JdbcConverter converter, AggregatePath path, Identifier defaultIdentifier,
			Function<AggregatePath, Object> valueProvider) {

		Identifier identifierToUse = defaultIdentifier;

		AggregatePath idDefiningParentPath = path.getIdDefiningParentPath();

		// note that the idDefiningParentPath might not itself have an id property, but have a combination of back
		// references and possibly keys, that form an id
		if (idDefiningParentPath.hasIdProperty()) {

			AggregatePath.ColumnInfos infos = path.getTableInfo().backReferenceColumnInfos();
			identifierToUse = infos.reduce(Identifier.empty(), (ap, ci) -> {

				RelationalPersistentProperty property = ap.getRequiredLeafProperty();
				return Identifier.of(ci.name(), valueProvider.apply(ap), converter.getColumnType(property));
			}, Identifier::withPart);
		}

		return identifierToUse;
	}

	/**
	 * Adds a qualifier to the identifier to build. A qualifier is a map key or a list index.
	 *
	 * @param path path to the map that gets qualified by {@code value}. Must not be {@literal null}.
	 * @param value map key or list index qualifying the map identified by {@code path}. Must not be {@literal null}.
	 * @return this builder. Guaranteed to be not {@literal null}.
	 */
	public JdbcIdentifierBuilder withQualifier(AggregatePath path, Object value) {

		Assert.notNull(path, "Path must not be null");
		Assert.notNull(value, "Value must not be null");

		AggregatePath.TableInfo tableInfo = path.getTableInfo();
		identifier = identifier.withPart(tableInfo.qualifierColumnInfo().name(), value, tableInfo.qualifierColumnType());

		return this;
	}

	public Identifier build() {
		return identifier;
	}
}
