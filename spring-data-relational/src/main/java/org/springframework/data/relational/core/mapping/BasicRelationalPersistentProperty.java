/*
 * Copyright 2017-2024 the original author or authors.
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
import org.springframework.data.spel.EvaluationContextProvider;
import org.springframework.data.util.Lazy;
import org.springframework.expression.Expression;
import org.springframework.expression.ParserContext;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;
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
 */
public class BasicRelationalPersistentProperty extends AnnotationBasedPersistentProperty<RelationalPersistentProperty>
		implements RelationalPersistentProperty {

	private static final SpelExpressionParser PARSER = new SpelExpressionParser();

	private final Lazy<SqlIdentifier> columnName;
	private final boolean hasExplicitColumnName;
	private final @Nullable Expression columnNameExpression;
	private final Lazy<Optional<SqlIdentifier>> collectionIdColumnName;
	private final Lazy<SqlIdentifier> collectionKeyColumnName;
	private final @Nullable Expression collectionKeyColumnNameExpression;
	private final boolean isEmbedded;

	private final String embeddedPrefix;
	private final NamingStrategy namingStrategy;
	private boolean forceQuote = true;
	private ExpressionEvaluator expressionEvaluator = new ExpressionEvaluator(EvaluationContextProvider.DEFAULT);

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

			collectionKeyColumnName = Lazy.of(
					() -> StringUtils.hasText(mappedCollection.keyColumn()) ? createSqlIdentifier(mappedCollection.keyColumn())
							: createDerivedSqlIdentifier(namingStrategy.getKeyColumn(this)));

			this.collectionKeyColumnNameExpression = detectExpression(mappedCollection.keyColumn());
		} else {

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

		if (collectionIdColumnName == null) {
			collectionIdColumnName = Lazy.of(Optional.empty());
		}

		this.collectionIdColumnName = collectionIdColumnName;
		this.collectionKeyColumnName = collectionKeyColumnName;
	}

	void setExpressionEvaluator(ExpressionEvaluator expressionEvaluator) {
		this.expressionEvaluator = expressionEvaluator;
	}

	/**
	 * Returns a SpEL {@link Expression} if the given {@link String} is actually an expression that does not evaluate to a
	 * {@link LiteralExpression} (indicating that no subsequent evaluation is necessary).
	 *
	 * @param potentialExpression can be {@literal null}
	 * @return can be {@literal null}.
	 */
	@Nullable
	private static Expression detectExpression(@Nullable String potentialExpression) {

		if (!StringUtils.hasText(potentialExpression)) {
			return null;
		}

		Expression expression = PARSER.parseExpression(potentialExpression, ParserContext.TEMPLATE_EXPRESSION);
		return expression instanceof LiteralExpression ? null : expression;
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

		return createSqlIdentifier(expressionEvaluator.evaluate(columnNameExpression));
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

		return collectionIdColumnName.get()
				.orElseGet(() -> createDerivedSqlIdentifier(this.namingStrategy.getReverseColumnName(owner)));
	}

	@Override
	public SqlIdentifier getKeyColumn() {

		if (!isQualified()) {
			return null;
		}

		if (collectionKeyColumnNameExpression == null) {
			return collectionKeyColumnName.get();
		}

		return createSqlIdentifier(expressionEvaluator.evaluate(collectionKeyColumnNameExpression));
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
		return isEmbedded;
	}

	@Override
	public String getEmbeddedPrefix() {
		return isEmbedded() ? embeddedPrefix : null;
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
