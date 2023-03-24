/*
 * Copyright 2017-2023 the original author or authors.
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
package org.springframework.data.relational.core.mapping;

import java.util.Optional;
import java.util.Set;

import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.model.AnnotationBasedPersistentProperty;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.relational.core.mapping.Embedded.OnEmpty;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.util.Lazy;
import org.springframework.data.util.Optionals;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Meta data about a property to be used by repository implementations.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 * @author Florian Lüdiger
 * @author Bastian Wilhelm
 * @author Kurt Niemi
 */
public class BasicRelationalPersistentProperty extends AnnotationBasedPersistentProperty<RelationalPersistentProperty>
		implements RelationalPersistentProperty {

	private final Lazy<SqlIdentifier> columnName;
	private final Lazy<Optional<SqlIdentifier>> collectionIdColumnName;
	private final Lazy<SqlIdentifier> collectionKeyColumnName;
	private final Lazy<Boolean> isEmbedded;
	private final Lazy<String> embeddedPrefix;
	private final NamingStrategy namingStrategy;
	private boolean forceQuote = true;
	private SpelExpressionProcessor spelExpressionProcessor = new SpelExpressionProcessor();

	/**
	 * Creates a new {@link BasicRelationalPersistentProperty}.
	 *
	 * @param property must not be {@literal null}.
	 * @param owner must not be {@literal null}.
	 * @param simpleTypeHolder must not be {@literal null}.
	 * @param context must not be {@literal null}
	 * @since 2.0, use
	 *        {@link #BasicRelationalPersistentProperty(Property, PersistentEntity, SimpleTypeHolder, NamingStrategy)}.
	 */
	@Deprecated
	public BasicRelationalPersistentProperty(Property property, PersistentEntity<?, RelationalPersistentProperty> owner,
			SimpleTypeHolder simpleTypeHolder, RelationalMappingContext context) {
		this(property, owner, simpleTypeHolder, context.getNamingStrategy());
	}

	/**
	 * Creates a new {@link BasicRelationalPersistentProperty}.
	 *
	 * @param property must not be {@literal null}.
	 * @param owner must not be {@literal null}.
	 * @param simpleTypeHolder must not be {@literal null}.
	 * @param namingStrategy must not be {@literal null}
	 * @since 2.0
	 */
	public BasicRelationalPersistentProperty(Property property, PersistentEntity<?, RelationalPersistentProperty> owner,
			SimpleTypeHolder simpleTypeHolder, NamingStrategy namingStrategy) {

		super(property, owner, simpleTypeHolder);
		this.namingStrategy = namingStrategy;

		Assert.notNull(namingStrategy, "NamingStrategy must not be null");

		this.isEmbedded = Lazy.of(() -> Optional.ofNullable(findAnnotation(Embedded.class)).isPresent());

		this.embeddedPrefix = Lazy.of(() -> Optional.ofNullable(findAnnotation(Embedded.class)) //
				.map(Embedded::prefix) //
				.orElse(""));

		this.columnName = Lazy.of(() -> Optional.ofNullable(findAnnotation(Column.class)) //
				.map(Column::value) //
				.map(spelExpressionProcessor::applySpelExpression) //
				.filter(StringUtils::hasText) //
				.map(this::createSqlIdentifier) //
				.orElseGet(() -> createDerivedSqlIdentifier(namingStrategy.getColumnName(this))));

		this.collectionIdColumnName = Lazy.of(() -> Optionals
				.toStream(Optional.ofNullable(findAnnotation(MappedCollection.class)) //
						.map(MappedCollection::idColumn), //
						Optional.ofNullable(findAnnotation(Column.class)) //
								.map(Column::value)) //
				.filter(StringUtils::hasText) //
				.findFirst() //
				.map(this::createSqlIdentifier)); //

		this.collectionKeyColumnName = Lazy.of(() -> Optionals //
				.toStream(Optional.ofNullable(findAnnotation(MappedCollection.class)).map(MappedCollection::keyColumn)) //
				.filter(StringUtils::hasText).findFirst() //
				.map(this::createSqlIdentifier) //
				.orElseGet(() -> createDerivedSqlIdentifier(namingStrategy.getKeyColumn(this))));
	}

	private SqlIdentifier createSqlIdentifier(String name) {
		return isForceQuote() ? SqlIdentifier.quoted(name) : SqlIdentifier.unquoted(name);
	}

	private SqlIdentifier createDerivedSqlIdentifier(String name) {
		return new DerivedSqlIdentifier(name, isForceQuote());
	}

	@Override
	protected Association<RelationalPersistentProperty> createAssociation() {
		return new Association<>(this, null);
	}

	public boolean isForceQuote() {
		return forceQuote;
	}

	public void setForceQuote(boolean forceQuote) {
		this.forceQuote = forceQuote;
	}

	@Override
	public boolean isEntity() {
		return super.isEntity() && !isAssociation();
	}

	@Override
	public SqlIdentifier getColumnName() {
		return columnName.get();
	}

	@Override
	public RelationalPersistentEntity<?> getOwner() {
		return (RelationalPersistentEntity<?>) super.getOwner();
	}

	@Override
	public SqlIdentifier getReverseColumnName(PersistentPropertyPathExtension path) {

		return collectionIdColumnName.get()
				.orElseGet(() -> createDerivedSqlIdentifier(this.namingStrategy.getReverseColumnName(path)));
	}

	@Override
	public SqlIdentifier getKeyColumn() {
		return isQualified() ? collectionKeyColumnName.get() : null;
	}

	@Override
	public boolean isQualified() {
		return isMap() || isListLike();
	}

	@Override
	public Class<?> getQualifierColumnType() {

		Assert.isTrue(isQualified(), "The qualifier column type is only defined for properties that are qualified");

		if (isMap()) {
			return getTypeInformation().getRequiredComponentType().getType();
		}

		// for lists and arrays
		return Integer.class;
	}

	@Override
	public boolean isOrdered() {
		return isListLike();
	}

	@Override
	public boolean isEmbedded() {
		return isEmbedded.get();
	}

	@Override
	public String getEmbeddedPrefix() {
		return isEmbedded() ? embeddedPrefix.get() : null;
	}

	@Override
	public boolean shouldCreateEmptyEmbedded() {

		Embedded findAnnotation = findAnnotation(Embedded.class);

		return findAnnotation != null && OnEmpty.USE_EMPTY.equals(findAnnotation.onEmpty());
	}

	@Override
	public boolean isInsertOnly() {
		return findAnnotation(InsertOnlyProperty.class) != null;
	}

	private boolean isListLike() {
		return isCollectionLike() && !Set.class.isAssignableFrom(this.getType());
	}

}
