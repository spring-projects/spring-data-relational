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
package org.springframework.data.jdbc.core;

import java.util.List;

import org.assertj.core.api.SoftAssertions;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.domain.PersistentPropertyPathExtension;

/**
 * @author Jens Schauder
 */
public class PersistentPropertyPathExtensionUnitTests {

	JdbcMappingContext context = new JdbcMappingContext();
	private RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(DummyEntity.class);

	@Test
	public void isEmbedded() {

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(extPath(entity).isEmbedded()).isFalse();
			softly.assertThat(extPath("second").isEmbedded()).isFalse();
			softly.assertThat(extPath("second.third2").isEmbedded()).isTrue();
		});
	}

	@Test
	public void isMultiValued() {

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(extPath(entity).isMultiValued()).isFalse();
			softly.assertThat(extPath("second").isMultiValued()).isFalse();
			softly.assertThat(extPath("second.third2").isMultiValued()).isFalse();
			softly.assertThat(extPath("secondList.third2").isMultiValued()).isTrue();
			softly.assertThat(extPath("secondList").isMultiValued()).isTrue();
		});
	}

	@Test
	public void leafEntity() {

		RelationalPersistentEntity<?> second = context.getRequiredPersistentEntity(Second.class);
		RelationalPersistentEntity<?> third = context.getRequiredPersistentEntity(Third.class);

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(extPath(entity).getLeafEntity()).isEqualTo(entity);
			softly.assertThat(extPath("second").getLeafEntity()).isEqualTo(second);
			softly.assertThat(extPath("second.third2").getLeafEntity()).isEqualTo(third);
			softly.assertThat(extPath("secondList.third2").getLeafEntity()).isEqualTo(third);
			softly.assertThat(extPath("secondList").getLeafEntity()).isEqualTo(second);
		});
	}

	@Test
	public void isEntity() {

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(extPath(entity).isEntity()).isTrue();
			String path = "second";
			softly.assertThat(extPath(path).isEntity()).isTrue();
			softly.assertThat(extPath("second.third2").isEntity()).isTrue();
			softly.assertThat(extPath("second.third2.value").isEntity()).isFalse();
			softly.assertThat(extPath("secondList.third2").isEntity()).isTrue();
			softly.assertThat(extPath("secondList.third2.value").isEntity()).isFalse();
			softly.assertThat(extPath("secondList").isEntity()).isTrue();
		});
	}

	@Test
	public void getTableName() {

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(extPath(entity).getTableName()).isEqualTo("dummy_entity");
			softly.assertThat(extPath("second").getTableName()).isEqualTo("second");
			softly.assertThat(extPath("second.third2").getTableName()).isEqualTo("second");
			softly.assertThat(extPath("second.third2.value").getTableName()).isEqualTo("second");
			softly.assertThat(extPath("secondList.third2").getTableName()).isEqualTo("second");
			softly.assertThat(extPath("secondList.third2.value").getTableName()).isEqualTo("second");
			softly.assertThat(extPath("secondList").getTableName()).isEqualTo("second");
		});
	}

	@Test
	public void getTableAlias() {

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(extPath(entity).getTableAlias()).isEqualTo(null);
			softly.assertThat(extPath("second").getTableAlias()).isEqualTo("second");
			softly.assertThat(extPath("second.third2").getTableAlias()).isEqualTo("second");
			softly.assertThat(extPath("second.third2.value").getTableAlias()).isEqualTo("second");
			softly.assertThat(extPath("second.third").getTableAlias()).isEqualTo("second_third");
			softly.assertThat(extPath("second.third.value").getTableAlias()).isEqualTo("second_third");
			softly.assertThat(extPath("secondList.third2").getTableAlias()).isEqualTo("secondList");
			softly.assertThat(extPath("secondList.third2.value").getTableAlias()).isEqualTo("secondList");
			softly.assertThat(extPath("secondList.third").getTableAlias()).isEqualTo("secondList_third");
			softly.assertThat(extPath("secondList.third.value").getTableAlias()).isEqualTo("secondList_third");
			softly.assertThat(extPath("secondList").getTableAlias()).isEqualTo("secondList");
			softly.assertThat(extPath("second2.third").getTableAlias()).isEqualTo("secthird");
		});
	}

	@Test
	public void getColumnName() {

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(extPath("second.third2.value").getColumnName()).isEqualTo("thrdvalue");
			softly.assertThat(extPath("second.third.value").getColumnName()).isEqualTo("value");
			softly.assertThat(extPath("secondList.third2.value").getColumnName()).isEqualTo("thrdvalue");
			softly.assertThat(extPath("secondList.third.value").getColumnName()).isEqualTo("value");
			softly.assertThat(extPath("second2.third2.value").getColumnName()).isEqualTo("secthrdvalue");
			softly.assertThat(extPath("second2.third.value").getColumnName()).isEqualTo("value");
		});
	}

	@Test // DATAJDBC-359
	public void idDefiningPath() {

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(extPath("second.third2.value").getIdDefiningParentPath().getLength()).isEqualTo(0);
			softly.assertThat(extPath("second.third.value").getIdDefiningParentPath().getLength()).isEqualTo(0);
			softly.assertThat(extPath("secondList.third2.value").getIdDefiningParentPath().getLength()).isEqualTo(0);
			softly.assertThat(extPath("secondList.third.value").getIdDefiningParentPath().getLength()).isEqualTo(0);
			softly.assertThat(extPath("second2.third2.value").getIdDefiningParentPath().getLength()).isEqualTo(0);
			softly.assertThat(extPath("second2.third.value").getIdDefiningParentPath().getLength()).isEqualTo(0);
			softly.assertThat(extPath("withId.second.third2.value").getIdDefiningParentPath().getLength()).isEqualTo(1);
			softly.assertThat(extPath("withId.second.third.value").getIdDefiningParentPath().getLength()).isEqualTo(1);
		});
	}

	@Test // DATAJDBC-359
	public void reverseColumnName() {

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(extPath("second.third2").getReverseColumnName()).isEqualTo("dummy_entity");
			softly.assertThat(extPath("second.third").getReverseColumnName()).isEqualTo("dummy_entity");
			softly.assertThat(extPath("secondList.third2").getReverseColumnName()).isEqualTo("dummy_entity");
			softly.assertThat(extPath("secondList.third").getReverseColumnName()).isEqualTo("dummy_entity");
			softly.assertThat(extPath("second2.third2").getReverseColumnName()).isEqualTo("dummy_entity");
			softly.assertThat(extPath("second2.third").getReverseColumnName()).isEqualTo("dummy_entity");
			softly.assertThat(extPath("withId.second.third2.value").getReverseColumnName()).isEqualTo("with_id");
			softly.assertThat(extPath("withId.second.third").getReverseColumnName()).isEqualTo("with_id");
			softly.assertThat(extPath("withId.second2.third").getReverseColumnName()).isEqualTo("with_id");
		});
	}

	@Test // DATAJDBC-359
	public void getRequiredIdProperty() {

		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(extPath(entity).getRequiredIdProperty().getName()).isEqualTo("entityId");
			softly.assertThat(extPath("withId").getRequiredIdProperty().getName()).isEqualTo("withIdId");
			softly.assertThatThrownBy(()->extPath("second").getRequiredIdProperty()).isInstanceOf(IllegalStateException.class);
		});
	}

	@Test // DATAJDBC-359
	public void extendBy() {


		SoftAssertions.assertSoftly(softly -> {

			softly.assertThat(extPath(entity).extendBy(entity.getRequiredPersistentProperty("withId"))).isEqualTo(extPath("withId"));
			softly.assertThat(extPath("withId").extendBy(extPath("withId").getRequiredIdProperty())).isEqualTo(extPath("withId.withIdId"));
		});
	}

	@NotNull
	private PersistentPropertyPathExtension extPath(RelationalPersistentEntity<?> entity) {
		return new PersistentPropertyPathExtension(context, entity);
	}

	@NotNull
	private PersistentPropertyPathExtension extPath(String path) {
		return new PersistentPropertyPathExtension(context, createSimplePath(path));
	}

	PersistentPropertyPath<RelationalPersistentProperty> createSimplePath(String path) {
		return PropertyPathTestingUtils.toPath(path, DummyEntity.class, context);
	}

	@SuppressWarnings("unused")
	static class DummyEntity {
		@Id Long entityId;
		Second second;
		@Embedded("sec") Second second2;
		List<Second> secondList;
		WithId withId;
	}

	@SuppressWarnings("unused")
	static class Second {
		Third third;
		@Embedded("thrd") Third third2;
	}

	@SuppressWarnings("unused")
	static class Third {
		String value;
	}
	@SuppressWarnings("unused")
	static class WithId {
		@Id Long withIdId;
		Second second;
		@Embedded("sec") Second second2;
	}

}
