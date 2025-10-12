/*
 * Copyright 2017-2025 the original author or authors.
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

import org.jspecify.annotations.Nullable;
import org.springframework.data.expression.ValueExpression;
import org.springframework.data.expression.ValueExpressionParser;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.model.AnnotationBasedPersistentProperty;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.relational.core.mapping.Embedded.OnEmpty;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.data.spel.EvaluationContextProvider;
import org.springframework.data.util.Lazy;
import org.springframework.expression.Expression;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * SQL-specific {@link org.springframework.data.mapping.PersistentProperty} implementation.
 *
 * @author Jens Schauder
 * @author Greg Turnquist
 * @author Florian Lüdiger
 * @author Bastian Wilhelm
 * @author Kurt Niemi
 * @author Sergey Korotaev
 * @author Mark Paluch
 */
public class BasicRelationalPersistentProperty extends AnnotationBasedPersistentProperty<RelationalPersistentProperty>
		implements RelationalPersistentProperty {

	private static final ValueExpressionParser PARSER = ValueExpressionParser.create();

	private final Lazy<SqlIdentifier> columnName;
	private final boolean hasExplicitColumnName;
	private final @Nullable ValueExpression columnNameExpression;
	private final @Nullable SqlIdentifier sequence;
	private final Lazy<Optional<SqlIdentifier>> collectionIdColumnName;
	private final @Nullable ValueExpression collectionIdColumnNameExpression;
	private final Lazy<SqlIdentifier> collectionKeyColumnName;
	private final @Nullable ValueExpression collectionKeyColumnNameExpression;
	private final boolean isEmbedded;
	private final String embeddedPrefix;

	private final NamingStrategy namingStrategy;
	private boolean forceQuote = true;

	private SqlIdentifierExpressionEvaluator sqlIdentifierExpressionEvaluator = new SqlIdentifierExpressionEvaluator(
			EvaluationContextProvider.DEFAULT);

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

		this.isEmbedded = isAnnotationPresent(Embedded.class);
		this.embeddedPrefix = Optional.ofNullable(findAnnotation(Embedded.class)) //
				.map(Embedded::prefix) //
				.orElse("");

		Lazy<Optional<SqlIdentifier>> collectionIdColumnName = null;
		Lazy<SqlIdentifier> collectionKeyColumnName = Lazy
				.of(() -> createDerivedSqlIdentifier(namingStrategy.getKeyColumn(this)));

		if (isAnnotationPresent(MappedCollection.class)) {

			MappedCollection mappedCollection = getRequiredAnnotation(MappedCollection.class);

			if (StringUtils.hasText(mappedCollection.idColumn())) {
				collectionIdColumnName = Lazy.of(() -> Optional.of(createSqlIdentifier(mappedCollection.idColumn())));
			}
			this.collectionIdColumnNameExpression = detectExpression(mappedCollection.idColumn());

			collectionKeyColumnName = Lazy.of(
					() -> StringUtils.hasText(mappedCollection.keyColumn()) ? createSqlIdentifier(mappedCollection.keyColumn())
							: createDerivedSqlIdentifier(namingStrategy.getKeyColumn(this)));

			this.collectionKeyColumnNameExpression = detectExpression(mappedCollection.keyColumn());
		} else {

			this.collectionIdColumnNameExpression = null;
			this.collectionKeyColumnNameExpression = null;
		}

		if (isAnnotationPresent(Column.class)) {

			Column column = getRequiredAnnotation(Column.class);
			this.hasExplicitColumnName = StringUtils.hasText(column.value());

			this.columnName = Lazy.of(() -> StringUtils.hasText(column.value()) ? createSqlIdentifier(column.value())
					: createDerivedSqlIdentifier(namingStrategy.getColumnName(this)));
			this.columnNameExpression = detectExpression(column.value());

			if (collectionIdColumnName == null && StringUtils.hasText(column.value())) {
				collectionIdColumnName = Lazy.of(() -> Optional.of(createSqlIdentifier(column.value())));
			}

		} else {
			this.hasExplicitColumnName = false;
			this.columnName = Lazy.of(() -> createDerivedSqlIdentifier(namingStrategy.getColumnName(this)));
			this.columnNameExpression = null;
		}

		this.sequence = determineSequenceName();

		if (collectionIdColumnName == null) {
			collectionIdColumnName = Lazy.of(Optional.empty());
		}

		this.collectionIdColumnName = collectionIdColumnName;
		this.collectionKeyColumnName = collectionKeyColumnName;
	}

	void setSqlIdentifierExpressionEvaluator(SqlIdentifierExpressionEvaluator sqlIdentifierExpressionEvaluator) {
		this.sqlIdentifierExpressionEvaluator = sqlIdentifierExpressionEvaluator;
	}

	/**
	 * Returns a SpEL {@link Expression} if the given {@link String} is actually an expression that does not evaluate to a
	 * {@link LiteralExpression} (indicating that no subsequent evaluation is necessary).
	 *
	 * @param potentialExpression can be {@literal null}
	 * @return can be {@literal null}.
	 */
	private static @Nullable ValueExpression detectExpression(@Nullable String potentialExpression) {

		if (!StringUtils.hasText(potentialExpression)) {
			return null;
		}

		ValueExpression expression = PARSER.parse(potentialExpression);
		return expression.isLiteral() ? null : expression;
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

		if (columnNameExpression == null) {
			return columnName.get();
		}

		return sqlIdentifierExpressionEvaluator.evaluate(columnNameExpression, isForceQuote());
	}

	@Override
	public boolean hasExplicitColumnName() {
		return hasExplicitColumnName;
	}

	@Override
	public RelationalPersistentEntity<?> getOwner() {
		return (RelationalPersistentEntity<?>) super.getOwner();
	}

	@Override
	public SqlIdentifier getReverseColumnName(RelationalPersistentEntity<?> owner) {

		if (collectionIdColumnNameExpression == null) {

			return collectionIdColumnName.get()
					.orElseGet(() -> createDerivedSqlIdentifier(this.namingStrategy.getReverseColumnName(owner)));
		}

		return sqlIdentifierExpressionEvaluator.evaluate(collectionIdColumnNameExpression, isForceQuote());
	}

	@Override
	public @Nullable SqlIdentifier getKeyColumn() {

		if (!isQualified()) {
			return null;
		}

		if (collectionKeyColumnNameExpression == null) {
			return collectionKeyColumnName.get();
		}

		return sqlIdentifierExpressionEvaluator.evaluate(collectionKeyColumnNameExpression, isForceQuote());
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
		return isEmbedded || isCompositeId();
	}

	private boolean isCompositeId() {
		return isIdProperty() && isEntity();
	}

	@Override
	public String getEmbeddedPrefix() {
		return isEmbedded() ? embeddedPrefix : "";
	}

	@Override
	public boolean shouldCreateEmptyEmbedded() {

		Embedded findAnnotation = findAnnotation(Embedded.class);

		return (findAnnotation != null && OnEmpty.USE_EMPTY.equals(findAnnotation.onEmpty()))
				|| (isIdProperty() && isEntity());
	}

	@Override
	public boolean isInsertOnly() {
		return findAnnotation(InsertOnlyProperty.class) != null;
	}

	@Override
	public @Nullable SqlIdentifier getSequence() {
		return this.sequence;
	}

	private boolean isListLike() {
		return isCollectionLike() && !Set.class.isAssignableFrom(this.getType());
	}

	private @Nullable SqlIdentifier determineSequenceName() {

		if (isAnnotationPresent(Sequence.class)) {

			Sequence annotation = getRequiredAnnotation(Sequence.class);

			String sequence = annotation.sequence();
			String schema = annotation.schema();

			SqlIdentifier sequenceIdentifier = SqlIdentifier.quoted(sequence);
			if (StringUtils.hasText(schema)) {
				sequenceIdentifier = SqlIdentifier.from(SqlIdentifier.quoted(schema), sequenceIdentifier);
			}

			return sequenceIdentifier;
		} else {
			return null;
		}
	}

}
