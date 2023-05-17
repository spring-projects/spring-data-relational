/*
 * Copyright 2019-2023 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.SoftAssertions.*;
import static org.springframework.data.relational.core.sql.SqlIdentifier.*;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.ReadOnlyProperty;
import org.springframework.data.jdbc.core.mapping.JdbcMappingContext;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.mapping.Embedded;
import org.springframework.data.relational.core.mapping.Embedded.OnEmpty;
import org.springframework.data.relational.core.mapping.PersistentPropertyPathExtension;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;

/**
 * @author Jens Schauder
 * @author Daniil Razorenov
 */
public class PersistentPropertyPathExtensionUnitTests {

	JdbcMappingContext context = new JdbcMappingContext();
	private RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(DummyEntity.class);

	@Test // DATAJDBC-340
	void isEmbedded() {

		assertSoftly(softly -> {

			softly.assertThat(extPath(entity).isEmbedded()).isFalse();
			softly.assertThat(extPath("second").isEmbedded()).isFalse();
			softly.assertThat(extPath("second.third2").isEmbedded()).isTrue();
		});
	}

	@Test // DATAJDBC-340
	void isMultiValued() {

		assertSoftly(softly -> {

			softly.assertThat(extPath(entity).isMultiValued()).isFalse();
			softly.assertThat(extPath("second").isMultiValued()).isFalse();
			softly.assertThat(extPath("second.third2").isMultiValued()).isFalse();
			softly.assertThat(extPath("secondList.third2").isMultiValued()).isTrue();
			softly.assertThat(extPath("secondList").isMultiValued()).isTrue();
		});
	}

	@Test // DATAJDBC-340
	void leafEntity() {

		RelationalPersistentEntity<?> second = context.getRequiredPersistentEntity(Second.class);
		RelationalPersistentEntity<?> third = context.getRequiredPersistentEntity(Third.class);

		assertSoftly(softly -> {

			softly.assertThat(extPath(entity).getLeafEntity()).isEqualTo(entity);
			softly.assertThat(extPath("second").getLeafEntity()).isEqualTo(second);
			softly.assertThat(extPath("second.third2").getLeafEntity()).isEqualTo(third);
			softly.assertThat(extPath("secondList.third2").getLeafEntity()).isEqualTo(third);
			softly.assertThat(extPath("secondList").getLeafEntity()).isEqualTo(second);
		});
	}

	@Test // DATAJDBC-340
	void isEntity() {

		assertSoftly(softly -> {

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

	@Test // DATAJDBC-340
	void getTableName() {

		assertSoftly(softly -> {

			softly.assertThat(extPath(entity).getQualifiedTableName()).isEqualTo(quoted("DUMMY_ENTITY"));
			softly.assertThat(extPath("second").getQualifiedTableName()).isEqualTo(quoted("SECOND"));
			softly.assertThat(extPath("second.third2").getQualifiedTableName()).isEqualTo(quoted("SECOND"));
			softly.assertThat(extPath("second.third2.value").getQualifiedTableName()).isEqualTo(quoted("SECOND"));
			softly.assertThat(extPath("secondList.third2").getQualifiedTableName()).isEqualTo(quoted("SECOND"));
			softly.assertThat(extPath("secondList.third2.value").getQualifiedTableName()).isEqualTo(quoted("SECOND"));
			softly.assertThat(extPath("secondList").getQualifiedTableName()).isEqualTo(quoted("SECOND"));
		});
	}

	@Test // DATAJDBC-340
	void getTableAlias() {

		assertSoftly(softly -> {

			softly.assertThat(extPath(entity).getTableAlias()).isEqualTo(null);
			softly.assertThat(extPath("second").getTableAlias()).isEqualTo(quoted("second"));
			softly.assertThat(extPath("second.third2").getTableAlias()).isEqualTo(quoted("second"));
			softly.assertThat(extPath("second.third2.value").getTableAlias()).isEqualTo(quoted("second"));
			softly.assertThat(extPath("second.third").getTableAlias()).isEqualTo(quoted("second_third"));
			softly.assertThat(extPath("second.third.value").getTableAlias()).isEqualTo(quoted("second_third"));
			softly.assertThat(extPath("secondList.third2").getTableAlias()).isEqualTo(quoted("secondList"));
			softly.assertThat(extPath("secondList.third2.value").getTableAlias()).isEqualTo(quoted("secondList"));
			softly.assertThat(extPath("secondList.third").getTableAlias()).isEqualTo(quoted("secondList_third"));
			softly.assertThat(extPath("secondList.third.value").getTableAlias()).isEqualTo(quoted("secondList_third"));
			softly.assertThat(extPath("secondList").getTableAlias()).isEqualTo(quoted("secondList"));
			softly.assertThat(extPath("second2.third").getTableAlias()).isEqualTo(quoted("secthird"));
			softly.assertThat(extPath("second3.third").getTableAlias()).isEqualTo(quoted("third"));
		});
	}

	@Test // DATAJDBC-340
	void getColumnName() {

		assertSoftly(softly -> {

			softly.assertThat(extPath("second.third2.value").getColumnName()).isEqualTo(quoted("THRDVALUE"));
			softly.assertThat(extPath("second.third.value").getColumnName()).isEqualTo(quoted("VALUE"));
			softly.assertThat(extPath("secondList.third2.value").getColumnName()).isEqualTo(quoted("THRDVALUE"));
			softly.assertThat(extPath("secondList.third.value").getColumnName()).isEqualTo(quoted("VALUE"));
			softly.assertThat(extPath("second2.third2.value").getColumnName()).isEqualTo(quoted("SECTHRDVALUE"));
			softly.assertThat(extPath("second2.third.value").getColumnName()).isEqualTo(quoted("VALUE"));
		});
	}

	@Test // DATAJDBC-359
	void idDefiningPath() {

		assertSoftly(softly -> {

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
	void reverseColumnName() {

		assertSoftly(softly -> {

			softly.assertThat(extPath("second.third2").getReverseColumnName()).isEqualTo(quoted("DUMMY_ENTITY"));
			softly.assertThat(extPath("second.third").getReverseColumnName()).isEqualTo(quoted("DUMMY_ENTITY"));
			softly.assertThat(extPath("secondList.third2").getReverseColumnName()).isEqualTo(quoted("DUMMY_ENTITY"));
			softly.assertThat(extPath("secondList.third").getReverseColumnName()).isEqualTo(quoted("DUMMY_ENTITY"));
			softly.assertThat(extPath("second2.third2").getReverseColumnName()).isEqualTo(quoted("DUMMY_ENTITY"));
			softly.assertThat(extPath("second2.third").getReverseColumnName()).isEqualTo(quoted("DUMMY_ENTITY"));
			softly.assertThat(extPath("withId.second.third2.value").getReverseColumnName()).isEqualTo(quoted("WITH_ID"));
			softly.assertThat(extPath("withId.second.third").getReverseColumnName()).isEqualTo(quoted("WITH_ID"));
			softly.assertThat(extPath("withId.second2.third").getReverseColumnName()).isEqualTo(quoted("WITH_ID"));
		});
	}

	@Test // DATAJDBC-359
	void getRequiredIdProperty() {

		assertSoftly(softly -> {

			softly.assertThat(extPath(entity).getRequiredIdProperty().getName()).isEqualTo("entityId");
			softly.assertThat(extPath("withId").getRequiredIdProperty().getName()).isEqualTo("withIdId");
			softly.assertThatThrownBy(() -> extPath("second").getRequiredIdProperty())
					.isInstanceOf(IllegalStateException.class);
		});
	}

	@Test // DATAJDBC-359
	void extendBy() {

		assertSoftly(softly -> {

			softly.assertThat(extPath(entity).extendBy(entity.getRequiredPersistentProperty("withId")))
					.isEqualTo(extPath("withId"));
			softly.assertThat(extPath("withId").extendBy(extPath("withId").getRequiredIdProperty()))
					.isEqualTo(extPath("withId.withIdId"));
		});
	}

	@Test // GH-1164
	void equalsWorks() {

		PersistentPropertyPathExtension root1 = extPath(entity);
		PersistentPropertyPathExtension root2 = extPath(entity);
		PersistentPropertyPathExtension path1 = extPath("withId");
		PersistentPropertyPathExtension path2 = extPath("withId");

		assertSoftly(softly -> {

			softly.assertThat(root1).describedAs("root is equal to self").isEqualTo(root1);
			softly.assertThat(root2).describedAs("root is equal to identical root").isEqualTo(root1);
			softly.assertThat(path1).describedAs("path is equal to self").isEqualTo(path1);
			softly.assertThat(path2).describedAs("path is equal to identical path").isEqualTo(path1);
			softly.assertThat(path1).describedAs("path is not equal to other path").isNotEqualTo(extPath("entityId"));
			softly.assertThat(root1).describedAs("root is not equal to path").isNotEqualTo(path1);
			softly.assertThat(path1).describedAs("path is not equal to root").isNotEqualTo(root1);
		});
	}

	@Test // GH-1249
	void isWritable() {

		assertSoftly(softly -> {
			softly.assertThat(PersistentPropertyPathExtension.isWritable(createSimplePath("withId")))
					.describedAs("simple path is writable").isTrue();
			softly.assertThat(PersistentPropertyPathExtension.isWritable(createSimplePath("secondList.third2")))
					.describedAs("long path is writable").isTrue();
			softly.assertThat(PersistentPropertyPathExtension.isWritable(createSimplePath("second")))
					.describedAs("simple read only path is not writable").isFalse();
			softly.assertThat(PersistentPropertyPathExtension.isWritable(createSimplePath("second.third")))
					.describedAs("long path containing read only element is not writable").isFalse();
		});
	}

	@Test // GH-1525
	void getAggregatePath() {
		assertThat(extPath("withId").getAggregatePath()).isNotNull();
	}
	@Test // GH-1525
	void getAggregatePathFromRoot() {
		assertThat(extPath(entity).getAggregatePath()).isNotNull();
	}
	private PersistentPropertyPathExtension extPath(RelationalPersistentEntity<?> entity) {
		return new PersistentPropertyPathExtension(context, entity);
	}

	private PersistentPropertyPathExtension extPath(String path) {
		return new PersistentPropertyPathExtension(context, createSimplePath(path));
	}

	PersistentPropertyPath<RelationalPersistentProperty> createSimplePath(String path) {
		return PersistentPropertyPathTestUtils.getPath(path, DummyEntity.class, context);
	}

	@SuppressWarnings("unused")
	static class DummyEntity {
		@Id Long entityId;
		@ReadOnlyProperty Second second;
		@Embedded(onEmpty = OnEmpty.USE_NULL, prefix = "sec") Second second2;
		@Embedded(onEmpty = OnEmpty.USE_NULL) Second second3;
		List<Second> secondList;
		WithId withId;
	}

	@SuppressWarnings("unused")
	static class Second {
		Third third;
		@Embedded(onEmpty = OnEmpty.USE_NULL, prefix = "thrd") Third third2;
	}

	@SuppressWarnings("unused")
	static class Third {
		String value;
	}

	@SuppressWarnings("unused")
	static class WithId {
		@Id Long withIdId;
		Second second;
		@Embedded(onEmpty = OnEmpty.USE_NULL, prefix = "sec") Second second2;
	}

}
