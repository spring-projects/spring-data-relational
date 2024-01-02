/*
 * Copyright 2018-2024 the original author or authors.
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
package org.springframework.data.jdbc.core.mapping;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.SoftAssertions.*;
import static org.springframework.data.relational.core.sql.SqlIdentifier.*;

import junit.framework.AssertionFailedError;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.AggregatePath;
import org.springframework.data.relational.core.mapping.BasicRelationalPersistentProperty;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.MappedCollection;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

/**
 * Unit tests for the {@link BasicRelationalPersistentProperty}.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 * @author Florian Lüdiger
 * @author Mark Paluch
 */
public class BasicJdbcPersistentPropertyUnitTests {

	RelationalMappingContext context = new JdbcMappingContext();
	RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(DummyEntity.class);

	@Test // DATAJDBC-106
	public void detectsAnnotatedColumnName() {

		assertThat(entity.getRequiredPersistentProperty("name").getColumnName()).isEqualTo(quoted("dummy_name"));
		assertThat(entity.getRequiredPersistentProperty("localDateTime").getColumnName())
				.isEqualTo(quoted("dummy_last_updated_at"));
	}

	@Test // DATAJDBC-218
	public void detectsAnnotatedColumnAndKeyName() {

		String propertyName = "someList";
		RelationalPersistentProperty listProperty = entity.getRequiredPersistentProperty(propertyName);
		AggregatePath path = getPersistentPropertyPath(DummyEntity.class, propertyName);

		assertThat(listProperty.getReverseColumnName(path.getRequiredBaseProperty().getOwner()))
				.isEqualTo(quoted("dummy_column_name"));
		assertThat(listProperty.getKeyColumn()).isEqualTo(quoted("dummy_key_column_name"));
	}

	@Test // DATAJDBC-331
	public void detectsReverseColumnNameFromColumnAnnotation() {

		String propertyName = "someList";
		RelationalPersistentProperty listProperty = context //
				.getRequiredPersistentEntity(WithCollections.class) //
				.getRequiredPersistentProperty(propertyName);
		AggregatePath path = getPersistentPropertyPath(DummyEntity.class, propertyName);

		assertThat(listProperty.getKeyColumn()).isEqualTo(quoted("WITH_COLLECTIONS_KEY"));
		assertThat(listProperty.getReverseColumnName(path.getRequiredBaseProperty().getOwner()))
				.isEqualTo(quoted("some_value"));
	}

	@Test // DATAJDBC-331
	public void detectsKeyColumnOverrideNameFromMappedCollectionAnnotation() {

		RelationalPersistentProperty listProperty = context //
				.getRequiredPersistentEntity(WithCollections.class) //
				.getRequiredPersistentProperty("overrideList");
		AggregatePath path = getPersistentPropertyPath(WithCollections.class, "overrideList");

		assertThat(listProperty.getKeyColumn()).isEqualTo(quoted("override_key"));
		assertThat(listProperty.getReverseColumnName(path.getRequiredBaseProperty().getOwner()))
				.isEqualTo(quoted("override_id"));
	}

	@Test // GH-938
	void considersAggregateReferenceAnAssociation() {

		RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(DummyEntity.class);

		assertSoftly(softly -> {

			softly.assertThat(entity.getRequiredPersistentProperty("reference").isAssociation()) //
					.as("reference") //
					.isTrue();

			softly.assertThat(entity.getRequiredPersistentProperty("id").isAssociation()) //
					.as("id") //
					.isFalse();
			softly.assertThat(entity.getRequiredPersistentProperty("someEnum").isAssociation()) //
					.as("someEnum") //
					.isFalse();
			softly.assertThat(entity.getRequiredPersistentProperty("localDateTime").isAssociation()) //
					.as("localDateTime") //
					.isFalse();
			softly.assertThat(entity.getRequiredPersistentProperty("zonedDateTime").isAssociation()) //
					.as("zonedDateTime") //
					.isFalse();
			softly.assertThat(entity.getRequiredPersistentProperty("listField").isAssociation()) //
					.as("listField") //
					.isFalse();
			softly.assertThat(entity.getRequiredPersistentProperty("uuid").isAssociation()) //
					.as("uuid") //
					.isFalse();
		});
	}

	private AggregatePath getPersistentPropertyPath(Class<?> type, String propertyName) {

		PersistentPropertyPath<RelationalPersistentProperty> path = context
				.findPersistentPropertyPaths(type, p -> p.getName().equals(propertyName)).getFirst()
				.orElseThrow(() -> new AssertionFailedError(String.format("Couldn't find path for '%s'", propertyName)));

		return context.getAggregatePath(path);
	}

	@SuppressWarnings("unused")
	private enum SomeEnum {
		ALPHA
	}

	@SuppressWarnings("unused")
	private static class DummyEntity {

		@Id private final Long id;
		private final SomeEnum someEnum;
		private final LocalDateTime localDateTime;
		private final ZonedDateTime zonedDateTime;
		private final AggregateReference<DummyEntity, Long> reference;
		private final List<String> listField;
		private final UUID uuid;

		@MappedCollection(idColumn = "dummy_column_name",
				keyColumn = "dummy_key_column_name") private List<Integer> someList;

		// DATACMNS-106
		private @Column("dummy_name") String name;

		private DummyEntity(Long id, SomeEnum someEnum, LocalDateTime localDateTime, ZonedDateTime zonedDateTime,
				AggregateReference<DummyEntity, Long> reference, List<String> listField, UUID uuid) {
			this.id = id;
			this.someEnum = someEnum;
			this.localDateTime = localDateTime;
			this.zonedDateTime = zonedDateTime;
			this.reference = reference;
			this.listField = listField;
			this.uuid = uuid;
		}

		@Column("dummy_last_updated_at")
		public LocalDateTime getLocalDateTime() {
			return localDateTime;
		}

		public void setListSetter(Integer integer) {

		}

		public List<Date> getListGetter() {
			return null;
		}

		public Long getId() {
			return this.id;
		}

		public SomeEnum getSomeEnum() {
			return this.someEnum;
		}

		public ZonedDateTime getZonedDateTime() {
			return this.zonedDateTime;
		}

		public AggregateReference<DummyEntity, Long> getReference() {
			return this.reference;
		}

		public List<String> getListField() {
			return this.listField;
		}

		public UUID getUuid() {
			return this.uuid;
		}

		public List<Integer> getSomeList() {
			return this.someList;
		}

		public String getName() {
			return this.name;
		}

		public void setSomeList(List<Integer> someList) {
			this.someList = someList;
		}

		public void setName(String name) {
			this.name = name;
		}
	}

	private static class WithCollections {

		@Column(value = "some_value") List<Integer> someList;

		@Column(value = "some_value") //
		@MappedCollection(idColumn = "override_id", keyColumn = "override_key") //
		List<Integer> overrideList;

		public List<Integer> getSomeList() {
			return this.someList;
		}

		public List<Integer> getOverrideList() {
			return this.overrideList;
		}

		public void setSomeList(List<Integer> someList) {
			this.someList = someList;
		}

		public void setOverrideList(List<Integer> overrideList) {
			this.overrideList = overrideList;
		}
	}
}
