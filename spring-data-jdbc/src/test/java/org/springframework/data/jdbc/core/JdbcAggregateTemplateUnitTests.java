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

import static java.util.Arrays.*;
import static java.util.Collections.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.jdbc.core.convert.DataAccessStrategy;
import org.springframework.data.jdbc.core.convert.JdbcConverter;
import org.springframework.data.jdbc.core.convert.MappingJdbcConverter;
import org.springframework.data.jdbc.core.convert.RelationResolver;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.relational.core.conversion.MutableAggregateChange;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.event.AfterConvertCallback;
import org.springframework.data.relational.core.mapping.event.AfterDeleteCallback;
import org.springframework.data.relational.core.mapping.event.AfterSaveCallback;
import org.springframework.data.relational.core.mapping.event.BeforeConvertCallback;
import org.springframework.data.relational.core.mapping.event.BeforeDeleteCallback;
import org.springframework.data.relational.core.mapping.event.BeforeSaveCallback;

/**
 * Unit tests for {@link JdbcAggregateTemplate}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Milan Milanov
 * @author Chirag Tailor
 */
@ExtendWith(MockitoExtension.class)
public class JdbcAggregateTemplateUnitTests {

	JdbcAggregateTemplate template;

	@Mock
	DataAccessStrategy dataAccessStrategy;
	@Mock
	ApplicationEventPublisher eventPublisher;
	@Mock
	RelationResolver relationResolver;
	@Mock
	EntityCallbacks callbacks;

	@BeforeEach
	public void setUp() {

		RelationalMappingContext mappingContext = new RelationalMappingContext();
		JdbcConverter converter = new MappingJdbcConverter(mappingContext, relationResolver);

		template = new JdbcAggregateTemplate(eventPublisher, mappingContext, converter, dataAccessStrategy);
		template.setEntityCallbacks(callbacks);

	}

	@Test // DATAJDBC-378
	public void findAllByIdMustNotAcceptNullArgumentForType() {
		assertThatThrownBy(() -> template.findAllById(singleton(23L), null)).isInstanceOf(IllegalArgumentException.class);
	}

	@Test // DATAJDBC-378
	public void findAllByIdMustNotAcceptNullArgumentForIds() {

		assertThatThrownBy(() -> template.findAllById(null, SampleEntity.class))
				.isInstanceOf(IllegalArgumentException.class);
	}

	@Test // DATAJDBC-378
	public void findAllByIdWithEmptyListMustReturnEmptyResult() {
		assertThat(template.findAllById(emptyList(), SampleEntity.class)).isEmpty();
	}

	@Test // DATAJDBC-393, GH-1291
	public void callbackOnSave() {

		SampleEntity first = new SampleEntity(null, "Alfred");
		SampleEntity second = new SampleEntity(23L, "Alfred E.");
		SampleEntity third = new SampleEntity(23L, "Neumann");

		when(callbacks.callback(any(Class.class), any(), any(Object[].class))).thenReturn(second, third);

		SampleEntity last = template.save(first);

		verify(callbacks).callback(BeforeConvertCallback.class, first);
		verify(callbacks).callback(eq(BeforeSaveCallback.class), eq(second), any(MutableAggregateChange.class));
		verify(callbacks).callback(AfterSaveCallback.class, third);
		assertThat(last).isEqualTo(third);
		verify(eventPublisher, times(3)).publishEvent(any(Object.class));
	}

	@Test // GH-1291
	public void doesNotEmitEvents() {

		SampleEntity first = new SampleEntity(null, "Alfred");
		SampleEntity second = new SampleEntity(23L, "Alfred E.");
		SampleEntity third = new SampleEntity(23L, "Neumann");

		when(callbacks.callback(any(Class.class), any(), any(Object[].class))).thenReturn(second, third);

		template.setEntityLifecycleEventsEnabled(false);
		template.save(first);

		verifyNoInteractions(eventPublisher);
	}

	@Test
		// GH-1137
	void savePreparesInstanceWithInitialVersion_onInsert() {

		EntityWithVersion entity = new EntityWithVersion(1L);
		when(callbacks.callback(any(), any(), any(Object[].class))).thenReturn(entity, entity);

		template.save(entity);

		ArgumentCaptor<Object> aggregateRootCaptor = ArgumentCaptor.forClass(Object.class);
		verify(callbacks).callback(eq(BeforeSaveCallback.class), aggregateRootCaptor.capture(), any());

		EntityWithVersion afterConvert = (EntityWithVersion) aggregateRootCaptor.getValue();
		assertThat(afterConvert.getVersion()).isEqualTo(0L);
	}

	@Test
		// GH-1137
	void savePreparesInstanceWithInitialVersion_onInsert_whenVersionPropertyIsImmutable() {

		EntityWithImmutableVersion entity = new EntityWithImmutableVersion(1L, null);
		when(callbacks.callback(any(), any(), any(Object[].class))).thenReturn(entity, entity);

		template.save(entity);

		ArgumentCaptor<Object> aggregateRootCaptor = ArgumentCaptor.forClass(Object.class);
		verify(callbacks).callback(eq(BeforeSaveCallback.class), aggregateRootCaptor.capture(), any());

		EntityWithImmutableVersion afterConvert = (EntityWithImmutableVersion) aggregateRootCaptor.getValue();
		assertThat(afterConvert.getVersion()).isEqualTo(0L);
	}

	@Test
		// GH-1137
	void savePreparesInstanceWithInitialVersion_onInsert_whenVersionPropertyIsPrimitiveType() {

		EntityWithPrimitiveVersion entity = new EntityWithPrimitiveVersion(1L);
		when(callbacks.callback(any(), any(), any(Object[].class))).thenReturn(entity, entity);

		template.save(entity);

		ArgumentCaptor<Object> aggregateRootCaptor = ArgumentCaptor.forClass(Object.class);
		verify(callbacks).callback(eq(BeforeSaveCallback.class), aggregateRootCaptor.capture(), any());

		EntityWithPrimitiveVersion afterConvert = (EntityWithPrimitiveVersion) aggregateRootCaptor.getValue();
		assertThat(afterConvert.getVersion()).isEqualTo(1L);
	}

	@Test
		// GH-1137
	void savePreparesInstanceWithInitialVersion_onInsert__whenVersionPropertyIsImmutableAndPrimitiveType() {

		EntityWithImmutablePrimitiveVersion entity = new EntityWithImmutablePrimitiveVersion(1L, 0L);
		when(callbacks.callback(any(), any(), any(Object[].class))).thenReturn(entity, entity);

		template.save(entity);

		ArgumentCaptor<Object> aggregateRootCaptor = ArgumentCaptor.forClass(Object.class);
		verify(callbacks).callback(eq(BeforeSaveCallback.class), aggregateRootCaptor.capture(), any());

		EntityWithImmutablePrimitiveVersion afterConvert = (EntityWithImmutablePrimitiveVersion) aggregateRootCaptor
				.getValue();
		assertThat(afterConvert.getVersion()).isEqualTo(1L);
	}

	@Test
		// GH-1137
	void savePreparesChangeWithPreviousVersion_onUpdate() {

		when(dataAccessStrategy.updateWithVersion(any(), any(), any())).thenReturn(true);
		EntityWithVersion entity = new EntityWithVersion(1L);
		entity.setVersion(1L);
		when(callbacks.callback(any(), any(), any(Object[].class))).thenReturn(entity, entity);

		template.save(entity);

		ArgumentCaptor<Object> aggregateChangeCaptor = ArgumentCaptor.forClass(Object.class);
		verify(callbacks).callback(eq(BeforeSaveCallback.class), any(), aggregateChangeCaptor.capture());

		MutableAggregateChange<?> aggregateChange = (MutableAggregateChange<?>) aggregateChangeCaptor.getValue();
		assertThat(aggregateChange.getPreviousVersion()).isEqualTo(1L);
	}

	@Test
		// GH-1137
	void savePreparesInstanceWithNextVersion_onUpdate() {

		when(dataAccessStrategy.updateWithVersion(any(), any(), any())).thenReturn(true);
		EntityWithVersion entity = new EntityWithVersion(1L);
		entity.setVersion(1L);
		when(callbacks.callback(any(), any(), any(Object[].class))).thenReturn(entity, entity);

		template.save(entity);

		ArgumentCaptor<Object> aggregateRootCaptor = ArgumentCaptor.forClass(Object.class);
		verify(callbacks).callback(eq(BeforeSaveCallback.class), aggregateRootCaptor.capture(), any());

		EntityWithVersion afterConvert = (EntityWithVersion) aggregateRootCaptor.getValue();
		assertThat(afterConvert.getVersion()).isEqualTo(2L);
	}

	@Test
		// GH-1137
	void savePreparesInstanceWithNextVersion_onUpdate_whenVersionPropertyIsImmutable() {

		when(dataAccessStrategy.updateWithVersion(any(), any(), any())).thenReturn(true);
		EntityWithImmutableVersion entity = new EntityWithImmutableVersion(1L, 1L);
		when(callbacks.callback(any(), any(), any(Object[].class))).thenReturn(entity, entity);

		template.save(entity);

		ArgumentCaptor<Object> aggregateRootCaptor = ArgumentCaptor.forClass(Object.class);
		verify(callbacks).callback(eq(BeforeSaveCallback.class), aggregateRootCaptor.capture(), any());
		EntityWithImmutableVersion afterConvert = (EntityWithImmutableVersion) aggregateRootCaptor.getValue();
		assertThat(afterConvert.getVersion()).isEqualTo(2L);
	}

	@Test
		// GH-1137
	void deletePreparesChangeWithPreviousVersion_onDeleteByInstance() {

		EntityWithImmutableVersion entity = new EntityWithImmutableVersion(1L, 1L);
		when(callbacks.callback(any(), any(), any(Object[].class))).thenReturn(entity, entity);

		template.delete(entity);

		ArgumentCaptor<Object> aggregateChangeCaptor = ArgumentCaptor.forClass(Object.class);
		verify(callbacks).callback(eq(BeforeDeleteCallback.class), any(), aggregateChangeCaptor.capture());

		MutableAggregateChange<?> aggregateChange = (MutableAggregateChange<?>) aggregateChangeCaptor.getValue();
		assertThat(aggregateChange.getPreviousVersion()).isEqualTo(1L);
	}

	@Test // DATAJDBC-393
	public void callbackOnDelete() {

		SampleEntity first = new SampleEntity(23L, "Alfred");
		SampleEntity second = new SampleEntity(23L, "Alfred E.");

		when(callbacks.callback(any(Class.class), any(), any())).thenReturn(second);

		template.delete(first);

		verify(callbacks).callback(eq(BeforeDeleteCallback.class), eq(first), any(MutableAggregateChange.class));
		verify(callbacks).callback(AfterDeleteCallback.class, second);
	}

	@Test // DATAJDBC-101
	public void callbackOnLoadSorted() {

		SampleEntity alfred1 = new SampleEntity(23L, "Alfred");
		SampleEntity alfred2 = new SampleEntity(23L, "Alfred E.");

		SampleEntity neumann1 = new SampleEntity(42L, "Neumann");
		SampleEntity neumann2 = new SampleEntity(42L, "Alfred E. Neumann");

		when(dataAccessStrategy.findAll(SampleEntity.class, Sort.by("name"))).thenReturn(asList(alfred1, neumann1));

		when(callbacks.callback(any(Class.class), eq(alfred1), any(Object[].class))).thenReturn(alfred2);
		when(callbacks.callback(any(Class.class), eq(neumann1), any(Object[].class))).thenReturn(neumann2);

		Iterable<SampleEntity> all = template.findAll(SampleEntity.class, Sort.by("name"));

		verify(callbacks).callback(AfterConvertCallback.class, alfred1);
		verify(callbacks).callback(AfterConvertCallback.class, neumann1);

		assertThat(all).containsExactly(alfred2, neumann2);
	}

	@Test // DATAJDBC-101
	public void callbackOnLoadPaged() {

		SampleEntity alfred1 = new SampleEntity(23L, "Alfred");
		SampleEntity alfred2 = new SampleEntity(23L, "Alfred E.");

		SampleEntity neumann1 = new SampleEntity(42L, "Neumann");
		SampleEntity neumann2 = new SampleEntity(42L, "Alfred E. Neumann");

		when(dataAccessStrategy.findAll(SampleEntity.class, PageRequest.of(0, 20))).thenReturn(asList(alfred1, neumann1));

		when(callbacks.callback(any(Class.class), eq(alfred1), any(Object[].class))).thenReturn(alfred2);
		when(callbacks.callback(any(Class.class), eq(neumann1), any(Object[].class))).thenReturn(neumann2);

		Iterable<SampleEntity> all = template.findAll(SampleEntity.class, PageRequest.of(0, 20));

		verify(callbacks).callback(AfterConvertCallback.class, alfred1);
		verify(callbacks).callback(AfterConvertCallback.class, neumann1);

		assertThat(all).containsExactly(alfred2, neumann2);
	}

	@Test // GH-1401
	public void saveAllWithEmptyListDoesNothing() {
		assertThat(template.saveAll(emptyList())).isEmpty();
	}

	@Test // GH-1401
	public void insertAllWithEmptyListDoesNothing() {
		assertThat(template.insertAll(emptyList())).isEmpty();
	}

	@Test // GH-1401
	public void updateAllWithEmptyListDoesNothing() {
		assertThat(template.updateAll(emptyList())).isEmpty();
	}

	@Test // GH-1401
	public void deleteAllWithEmptyListDoesNothing() {
		template.deleteAll(emptyList());
	}

	@Test // GH-1401
	public void deleteAllByIdWithEmptyListDoesNothing() {
		template.deleteAllById(emptyList(), SampleEntity.class);
	}

	private static class SampleEntity {

		@Column("id1")
		@Id
		private Long id;

		private String name;

		public SampleEntity(Long id, String name) {
			this.id = id;
			this.name = name;
		}

		public Long getId() {
			return this.id;
		}

		public String getName() {
			return this.name;
		}

		public void setId(Long id) {
			this.id = id;
		}

		public void setName(String name) {
			this.name = name;
		}
	}

	private static class EntityWithVersion {

		@Column("id1")
		@Id
		private final Long id;

		@Version
		private Long version;

		public EntityWithVersion(Long id) {
			this.id = id;
		}

		public Long getId() {
			return this.id;
		}

		public Long getVersion() {
			return this.version;
		}

		public void setVersion(Long version) {
			this.version = version;
		}
	}

	private static class EntityWithImmutableVersion {

		@Column("id1")
		@Id
		private final Long id;

		@Version
		private final Long version;

		public EntityWithImmutableVersion(Long id, Long version) {
			this.id = id;
			this.version = version;
		}

		public Long getId() {
			return this.id;
		}

		public Long getVersion() {
			return this.version;
		}
	}

	private static class EntityWithPrimitiveVersion {

		@Column("id1")
		@Id
		private final Long id;

		@Version
		private long version;

		public EntityWithPrimitiveVersion(Long id) {
			this.id = id;
		}

		public Long getId() {
			return this.id;
		}

		public long getVersion() {
			return this.version;
		}

		public void setVersion(long version) {
			this.version = version;
		}
	}

	private static class EntityWithImmutablePrimitiveVersion {

		@Column("id1")
		@Id
		private final Long id;

		@Version
		private final long version;

		public EntityWithImmutablePrimitiveVersion(Long id, long version) {
			this.id = id;
			this.version = version;
		}

		public Long getId() {
			return this.id;
		}

		public long getVersion() {
			return this.version;
		}
	}
}
