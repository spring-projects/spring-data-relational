/*
 * Copyright 2023-2024 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.SoftAssertions.*;
import static org.springframework.data.relational.core.sql.SqlIdentifier.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.ReadOnlyProperty;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.relational.core.sql.SqlIdentifier;

/**
 * Tests for {@link AggregatePath}.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 */
class DefaultAggregatePathUnitTests {
	RelationalMappingContext context = new RelationalMappingContext();

	private RelationalPersistentEntity<?> entity = context.getRequiredPersistentEntity(DummyEntity.class);

	@Test // GH-1525
	void isNotRootForNonRootPath() {

		AggregatePath path = context.getAggregatePath(context.getPersistentPropertyPath("entityId", DummyEntity.class));

		assertThat(path.isRoot()).isFalse();
	}

	@Test // GH-1525
	void isRootForRootPath() {

		AggregatePath path = context.getAggregatePath(entity);

		assertThat(path.isRoot()).isTrue();
	}

	@Test // GH-1525
	void getParentPath() {

		assertSoftly(softly -> {

			softly.assertThat(path("second.third2.value").getParentPath()).isEqualTo(path("second.third2"));
			softly.assertThat(path("second.third2").getParentPath()).isEqualTo(path("second"));
			softly.assertThat(path("second").getParentPath()).isEqualTo(path());

			softly.assertThatThrownBy(() -> path().getParentPath()).isInstanceOf(IllegalStateException.class);
		});
	}

	@Test // GH-1525
	void getRequiredLeafEntity() {

		assertSoftly(softly -> {

			softly.assertThat(path().getRequiredLeafEntity()).isEqualTo(entity);
			softly.assertThat(path("second").getRequiredLeafEntity())
					.isEqualTo(context.getRequiredPersistentEntity(Second.class));
			softly.assertThat(path("second.third").getRequiredLeafEntity())
					.isEqualTo(context.getRequiredPersistentEntity(Third.class));
			softly.assertThat(path("secondList").getRequiredLeafEntity())
					.isEqualTo(context.getRequiredPersistentEntity(Second.class));

			softly.assertThatThrownBy(() -> path("secondList.third.value").getRequiredLeafEntity())
					.isInstanceOf(IllegalStateException.class);

		});
	}

	@Test // GH-1525
	void idDefiningPath() {

		assertSoftly(softly -> {

			softly.assertThat(path("second.third2.value").getIdDefiningParentPath()).isEqualTo(path());
			softly.assertThat(path("second.third.value").getIdDefiningParentPath()).isEqualTo(path());
			softly.assertThat(path("secondList.third2.value").getIdDefiningParentPath()).isEqualTo(path());
			softly.assertThat(path("secondList.third.value").getIdDefiningParentPath()).isEqualTo(path());
			softly.assertThat(path("second2.third2.value").getIdDefiningParentPath()).isEqualTo(path());
			softly.assertThat(path("second2.third.value").getIdDefiningParentPath()).isEqualTo(path());
			softly.assertThat(path("withId.second.third2.value").getIdDefiningParentPath()).isEqualTo(path("withId"));
			softly.assertThat(path("withId.second.third.value").getIdDefiningParentPath()).isEqualTo(path("withId"));
		});
	}

	@Test // GH-1525
	void getRequiredIdProperty() {

		assertSoftly(softly -> {

			softly.assertThat(path().getRequiredIdProperty().getName()).isEqualTo("entityId");
			softly.assertThat(path("withId").getRequiredIdProperty().getName()).isEqualTo("withIdId");
			softly.assertThatThrownBy(() -> path("second").getRequiredIdProperty()).isInstanceOf(IllegalStateException.class);
		});
	}

	@Test // GH-1525
	void reverseColumnName() {

		assertSoftly(softly -> {

			softly.assertThat(path("second.third2").getTableInfo().reverseColumnInfo().name())
					.isEqualTo(quoted("DUMMY_ENTITY"));
			softly.assertThat(path("second.third").getTableInfo().reverseColumnInfo().name())
					.isEqualTo(quoted("DUMMY_ENTITY"));
			softly.assertThat(path("secondList.third2").getTableInfo().reverseColumnInfo().name())
					.isEqualTo(quoted("DUMMY_ENTITY"));
			softly.assertThat(path("secondList.third").getTableInfo().reverseColumnInfo().name())
					.isEqualTo(quoted("DUMMY_ENTITY"));
			softly.assertThat(path("second2.third").getTableInfo().reverseColumnInfo().name())
					.isEqualTo(quoted("DUMMY_ENTITY"));
			softly.assertThat(path("withId.second.third2.value").getTableInfo().reverseColumnInfo().name())
					.isEqualTo(quoted("WITH_ID"));
			softly.assertThat(path("withId.second.third").getTableInfo().reverseColumnInfo().name())
					.isEqualTo(quoted("WITH_ID"));
			softly.assertThat(path("withId.second2.third").getTableInfo().reverseColumnInfo().name())
					.isEqualTo(quoted("WITH_ID"));
		});
	}

	@Test // GH-1525
	void getQualifierColumn() {

		assertSoftly(softly -> {

			softly.assertThat(path().getTableInfo().qualifierColumnInfo()).isEqualTo(null);
			softly.assertThat(path("second.third").getTableInfo().qualifierColumnInfo()).isEqualTo(null);
			softly.assertThat(path("secondList.third2").getTableInfo().qualifierColumnInfo()).isEqualTo(null);
			softly.assertThat(path("secondList").getTableInfo().qualifierColumnInfo().name())
					.isEqualTo(SqlIdentifier.quoted("DUMMY_ENTITY_KEY"));

		});
	}

	@Test // GH-1525
	void getQualifierColumnType() {

		assertSoftly(softly -> {

			softly.assertThat(path().getTableInfo().qualifierColumnType()).isEqualTo(null);
			softly.assertThat(path("second.third").getTableInfo().qualifierColumnType()).isEqualTo(null);
			softly.assertThat(path("secondList.third2").getTableInfo().qualifierColumnType()).isEqualTo(null);
			softly.assertThat(path("secondList").getTableInfo().qualifierColumnType()).isEqualTo(Integer.class);

		});
	}

	@Test // GH-1525
	void extendBy() {

		assertSoftly(softly -> {

			softly.assertThat(path().append(entity.getRequiredPersistentProperty("withId"))).isEqualTo(path("withId"));
			softly.assertThat(path("withId").append(path("withId").getRequiredIdProperty()))
					.isEqualTo(path("withId.withIdId"));
		});
	}

	@Test // GH-1525
	void isWritable() {

		assertSoftly(softly -> {
			softly.assertThat(context.getAggregatePath(createSimplePath("withId")).isWritable())
					.describedAs("simple path is writable").isTrue();
			softly.assertThat(context.getAggregatePath(createSimplePath("secondList.third2")).isWritable())
					.describedAs("long path is writable").isTrue();
			softly.assertThat(context.getAggregatePath(createSimplePath("second")).isWritable())
					.describedAs("simple read only path is not writable").isFalse();
			softly.assertThat(context.getAggregatePath(createSimplePath("second.third")).isWritable())
					.describedAs("long path containing read only element is not writable").isFalse();
		});
	}

	@Test // GH-1525
	void isEmbedded() {

		assertSoftly(softly -> {
			softly.assertThat(path().isEmbedded()).isFalse();
			softly.assertThat(path("withId").isEmbedded()).isFalse();
			softly.assertThat(path("second2.third").isEmbedded()).isFalse();
			softly.assertThat(path("second2.third2").isEmbedded()).isTrue();
			softly.assertThat(path("second2").isEmbedded()).isTrue();
		});
	}

	@Test // GH-1525
	void isEntity() {

		assertSoftly(softly -> {

			softly.assertThat(path().isEntity()).isTrue();
			softly.assertThat(path("second").isEntity()).isTrue();
			softly.assertThat(path("second.third2").isEntity()).isTrue();
			softly.assertThat(path("secondList.third2").isEntity()).isTrue();
			softly.assertThat(path("secondList").isEntity()).isTrue();
			softly.assertThat(path("second.third2.value").isEntity()).isFalse();
			softly.assertThat(path("secondList.third2.value").isEntity()).isFalse();
		});
	}

	@Test // GH-1525
	void isMultiValued() {

		assertSoftly(softly -> {

			softly.assertThat(path().isMultiValued()).isFalse();
			softly.assertThat(path("second").isMultiValued()).isFalse();
			softly.assertThat(path("second.third2").isMultiValued()).isFalse();
			softly.assertThat(path("secondList.third2").isMultiValued()).isTrue(); // this seems wrong as third2 is an
																																							// embedded path into Second, held by
																																							// List<Second> (so the parent is
																																							// multi-valued but not third2).
			// TODO: This test fails because MultiValued considers parents.
			// softly.assertThat(path("secondList.third.value").isMultiValued()).isFalse();
			softly.assertThat(path("secondList").isMultiValued()).isTrue();
		});
	}

	@Test // GH-1525
	void isQualified() {

		assertSoftly(softly -> {

			softly.assertThat(path().isQualified()).isFalse();
			softly.assertThat(path("second").isQualified()).isFalse();
			softly.assertThat(path("second.third2").isQualified()).isFalse();
			softly.assertThat(path("secondList.third2").isQualified()).isFalse();
			softly.assertThat(path("secondList").isQualified()).isTrue();
		});
	}

	@Test // GH-1525
	void isMap() {

		assertSoftly(softly -> {

			softly.assertThat(path().isMap()).isFalse();
			softly.assertThat(path("second").isMap()).isFalse();
			softly.assertThat(path("second.third2").isMap()).isFalse();
			softly.assertThat(path("secondList.third2").isMap()).isFalse();
			softly.assertThat(path("secondList").isMap()).isFalse();
			softly.assertThat(path("secondMap.third2").isMap()).isFalse();
			softly.assertThat(path("secondMap").isMap()).isTrue();
		});
	}

	@Test // GH-1525
	void isCollectionLike() {

		assertSoftly(softly -> {

			softly.assertThat(path().isCollectionLike()).isFalse();
			softly.assertThat(path("second").isCollectionLike()).isFalse();
			softly.assertThat(path("second.third2").isCollectionLike()).isFalse();
			softly.assertThat(path("secondList.third2").isCollectionLike()).isFalse();
			softly.assertThat(path("secondMap.third2").isCollectionLike()).isFalse();
			softly.assertThat(path("secondMap").isCollectionLike()).isFalse();
			softly.assertThat(path("secondList").isCollectionLike()).isTrue();
		});
	}

	@Test // GH-1525
	void isOrdered() {

		assertSoftly(softly -> {

			softly.assertThat(path().isOrdered()).isFalse();
			softly.assertThat(path("second").isOrdered()).isFalse();
			softly.assertThat(path("second.third2").isOrdered()).isFalse();
			softly.assertThat(path("secondList.third2").isOrdered()).isFalse();
			softly.assertThat(path("secondMap.third2").isOrdered()).isFalse();
			softly.assertThat(path("secondMap").isOrdered()).isFalse();
			softly.assertThat(path("secondList").isOrdered()).isTrue();
		});
	}

	@Test // GH-1525
	void getTableAlias() {

		assertSoftly(softly -> {

			softly.assertThat(path().getTableInfo().tableAlias()).isEqualTo(null);
			softly.assertThat(path("second").getTableInfo().tableAlias()).isEqualTo(quoted("second"));
			softly.assertThat(path("second.third2").getTableInfo().tableAlias()).isEqualTo(quoted("second"));
			softly.assertThat(path("second.third2.value").getTableInfo().tableAlias()).isEqualTo(quoted("second"));
			softly.assertThat(path("second.third").getTableInfo().tableAlias()).isEqualTo(quoted("second_third")); // missing
																																																							// _
			softly.assertThat(path("second.third.value").getTableInfo().tableAlias()).isEqualTo(quoted("second_third")); // missing
																																																										// _
			softly.assertThat(path("secondList.third2").getTableInfo().tableAlias()).isEqualTo(quoted("secondList"));
			softly.assertThat(path("secondList.third2.value").getTableInfo().tableAlias()).isEqualTo(quoted("secondList"));
			softly.assertThat(path("secondList.third").getTableInfo().tableAlias()).isEqualTo(quoted("secondList_third")); // missing
																																																											// _
			softly.assertThat(path("secondList.third.value").getTableInfo().tableAlias())
					.isEqualTo(quoted("secondList_third")); // missing _
			softly.assertThat(path("secondList").getTableInfo().tableAlias()).isEqualTo(quoted("secondList"));
			softly.assertThat(path("second2.third").getTableInfo().tableAlias()).isEqualTo(quoted("secthird"));
			softly.assertThat(path("second3.third").getTableInfo().tableAlias()).isEqualTo(quoted("third"));
		});
	}

	@Test // GH-1525
	void getTableName() {

		assertSoftly(softly -> {

			softly.assertThat(path().getTableInfo().qualifiedTableName()).isEqualTo(quoted("DUMMY_ENTITY"));
			softly.assertThat(path("second").getTableInfo().qualifiedTableName()).isEqualTo(quoted("SECOND"));
			softly.assertThat(path("second.third2").getTableInfo().qualifiedTableName()).isEqualTo(quoted("SECOND"));
			softly.assertThat(path("second.third2.value").getTableInfo().qualifiedTableName()).isEqualTo(quoted("SECOND"));
			softly.assertThat(path("secondList.third2").getTableInfo().qualifiedTableName()).isEqualTo(quoted("SECOND"));
			softly.assertThat(path("secondList.third2.value").getTableInfo().qualifiedTableName())
					.isEqualTo(quoted("SECOND"));
			softly.assertThat(path("secondList").getTableInfo().qualifiedTableName()).isEqualTo(quoted("SECOND"));
		});
	}

	@Test // GH-1525
	void getColumnName() {

		assertSoftly(softly -> {

			softly.assertThat(path("second.third2.value").getColumnInfo().name()).isEqualTo(quoted("THRDVALUE"));
			softly.assertThat(path("second.third.value").getColumnInfo().name()).isEqualTo(quoted("VALUE"));
			softly.assertThat(path("secondList.third2.value").getColumnInfo().name()).isEqualTo(quoted("THRDVALUE"));
			softly.assertThat(path("secondList.third.value").getColumnInfo().name()).isEqualTo(quoted("VALUE"));
			softly.assertThat(path("second2.third2.value").getColumnInfo().name()).isEqualTo(quoted("SECTHRDVALUE"));
			softly.assertThat(path("second2.third.value").getColumnInfo().name()).isEqualTo(quoted("VALUE"));
		});
	}

	@Test // GH-1525
	void getColumnAlias() {

		assertSoftly(softly -> {

			softly.assertThat(path("second.third2.value").getColumnInfo().alias()).isEqualTo(quoted("SECOND_THRDVALUE"));
			softly.assertThat(path("second.third.value").getColumnInfo().alias()).isEqualTo(quoted("SECOND_THIRD_VALUE"));
			softly.assertThat(path("secondList.third2.value").getColumnInfo().alias())
					.isEqualTo(quoted("SECONDLIST_THRDVALUE"));
			softly.assertThat(path("secondList.third.value").getColumnInfo().alias())
					.isEqualTo(quoted("SECONDLIST_THIRD_VALUE"));
			softly.assertThat(path("second2.third2.value").getColumnInfo().alias()).isEqualTo(quoted("SECTHRDVALUE"));
			softly.assertThat(path("second2.third.value").getColumnInfo().alias()).isEqualTo(quoted("SECTHIRD_VALUE"));
		});
	}

	@Test // GH-1525
	void getReverseColumnAlias() {

		assertSoftly(softly -> {

			softly.assertThat(path("second.third2.value").getTableInfo().reverseColumnInfo().alias())
					.isEqualTo(quoted("SECOND_DUMMY_ENTITY"));
			softly.assertThat(path("second.third.value").getTableInfo().reverseColumnInfo().alias())
					.isEqualTo(quoted("SECOND_THIRD_DUMMY_ENTITY"));
			softly.assertThat(path("secondList.third2.value").getTableInfo().reverseColumnInfo().alias())
					.isEqualTo(quoted("SECONDLIST_DUMMY_ENTITY"));
			softly.assertThat(path("secondList.third.value").getTableInfo().reverseColumnInfo().alias())
					.isEqualTo(quoted("SECONDLIST_THIRD_DUMMY_ENTITY"));
			softly.assertThat(path("second2.third.value").getTableInfo().reverseColumnInfo().alias())
					.isEqualTo(quoted("SECTHIRD_DUMMY_ENTITY"));
		});
	}

	@Test // GH-1525
	void getRequiredLeafProperty() {

		assertSoftly(softly -> {

			RelationalPersistentProperty prop = path("second.third2.value").getRequiredLeafProperty();
			softly.assertThat(prop.getName()).isEqualTo("value");
			softly.assertThat(prop.getOwner().getType()).isEqualTo(Third.class);
			softly.assertThat(path("second.third").getRequiredLeafProperty())
					.isEqualTo(context.getRequiredPersistentEntity(Second.class).getPersistentProperty("third"));
			softly.assertThat(path("secondList").getRequiredLeafProperty())
					.isEqualTo(entity.getPersistentProperty("secondList"));
			softly.assertThatThrownBy(() -> path().getRequiredLeafProperty()).isInstanceOf(IllegalStateException.class);
		});
	}

	@Test // GH-1525
	void getBaseProperty() {

		assertSoftly(softly -> {

			softly.assertThat(path("second.third2.value").getRequiredBaseProperty())
					.isEqualTo(entity.getPersistentProperty("second"));
			softly.assertThat(path("second.third.value").getRequiredBaseProperty())
					.isEqualTo(entity.getPersistentProperty("second"));
			softly.assertThat(path("secondList.third2.value").getRequiredBaseProperty())
					.isEqualTo(entity.getPersistentProperty("secondList"));
			softly.assertThatThrownBy(() -> path().getRequiredBaseProperty()).isInstanceOf(IllegalStateException.class);
		});
	}

	@Test // GH-1525
	void getIdColumnName() {

		assertSoftly(softly -> {

			softly.assertThat(path().getTableInfo().idColumnName()).isEqualTo(quoted("ENTITY_ID"));
			softly.assertThat(path("withId").getTableInfo().idColumnName()).isEqualTo(quoted("WITH_ID_ID"));

			softly.assertThat(path("second").getTableInfo().idColumnName()).isNull();
			softly.assertThat(path("second.third2").getTableInfo().idColumnName()).isNull();
			softly.assertThat(path("withId.second").getTableInfo().idColumnName()).isNull();
		});
	}

	@Test // GH-1525
	void toDotPath() {

		assertSoftly(softly -> {

			softly.assertThat(path().toDotPath()).isEqualTo("");
			softly.assertThat(path("second.third.value").toDotPath()).isEqualTo("second.third.value");
		});
	}

	@Test // GH-1525
	void getRequiredPersistentPropertyPath() {

		assertSoftly(softly -> {

			softly.assertThat(path("second.third.value").getRequiredPersistentPropertyPath())
					.isEqualTo(createSimplePath("second.third.value"));
			softly.assertThatThrownBy(() -> path().getRequiredPersistentPropertyPath())
					.isInstanceOf(IllegalStateException.class);
		});
	}

	@Test // GH-1525
	void getEffectiveIdColumnName() {

		assertSoftly(softly -> {

			softly.assertThat(path().getTableInfo().effectiveIdColumnName()).isEqualTo(quoted("ENTITY_ID"));
			softly.assertThat(path("second.third2").getTableInfo().effectiveIdColumnName()).isEqualTo(quoted("DUMMY_ENTITY"));
			softly.assertThat(path("withId.second.third").getTableInfo().effectiveIdColumnName())
					.isEqualTo(quoted("WITH_ID"));
			softly.assertThat(path("withId.second.third2.value").getTableInfo().effectiveIdColumnName())
					.isEqualTo(quoted("WITH_ID"));
		});
	}

	@Test // GH-1525
	void getLength() {

		assertThat(path().getLength()).isEqualTo(1);
		assertThat(path().stream().collect(Collectors.toList())).hasSize(1);

		assertThat(path("second.third2").getLength()).isEqualTo(3);
		assertThat(path("second.third2").stream().collect(Collectors.toList())).hasSize(3);

		assertThat(path("withId.second.third").getLength()).isEqualTo(4);
		assertThat(path("withId.second.third2.value").getLength()).isEqualTo(5);
	}

	private AggregatePath path() {
		return context.getAggregatePath(entity);
	}

	private AggregatePath path(String path) {
		return context.getAggregatePath(createSimplePath(path));
	}

	PersistentPropertyPath<RelationalPersistentProperty> createSimplePath(String path) {
		return PersistentPropertyPathTestUtils.getPath(context, path, DummyEntity.class);
	}

	@SuppressWarnings("unused")
	static class DummyEntity {
		@Id Long entityId;
		@ReadOnlyProperty Second second;
		@Embedded(onEmpty = Embedded.OnEmpty.USE_NULL, prefix = "sec") Second second2;
		@Embedded(onEmpty = Embedded.OnEmpty.USE_NULL) Second second3;
		List<Second> secondList;
		Map<String, Second> secondMap;
		WithId withId;
	}

	@SuppressWarnings("unused")
	static class Second {
		Third third;
		@Embedded(onEmpty = Embedded.OnEmpty.USE_NULL, prefix = "thrd") Third third2;
	}

	@SuppressWarnings("unused")
	static class Third {
		String value;
	}

	@SuppressWarnings("unused")
	static class WithId {
		@Id Long withIdId;
		Second second;
		@Embedded(onEmpty = Embedded.OnEmpty.USE_NULL, prefix = "sec") Second second2;
	}

}
