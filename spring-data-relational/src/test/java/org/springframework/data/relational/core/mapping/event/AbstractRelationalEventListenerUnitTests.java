/*
 * Copyright 2020-2024 the original author or authors.
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
package org.springframework.data.relational.core.mapping.event;

import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.data.relational.core.conversion.MutableAggregateChange;

/**
 * Unit tests for {@link AbstractRelationalEventListener}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 */
public class AbstractRelationalEventListenerUnitTests {

	List<String> events = new ArrayList<>();
	EventListenerUnderTest listener = new EventListenerUnderTest();
	DummyEntity dummyEntity = new DummyEntity();

	@Test // GH-1053
	public void afterConvert() {

		listener.onApplicationEvent(new AfterConvertEvent<>(dummyEntity));

		assertThat(events).containsExactly("afterConvert");
	}

	@Test // DATAJDBC-454
	public void beforeConvert() {

		listener.onApplicationEvent(new BeforeConvertEvent<>(dummyEntity));

		assertThat(events).containsExactly("beforeConvert");
	}

	@Test // DATAJDBC-454
	public void beforeSave() {

		listener.onApplicationEvent(new BeforeSaveEvent<>(dummyEntity, MutableAggregateChange.forSave(dummyEntity)));

		assertThat(events).containsExactly("beforeSave");
	}

	@Test // DATAJDBC-454
	public void afterSave() {

		listener.onApplicationEvent(new AfterSaveEvent<>(dummyEntity, MutableAggregateChange.forDelete(dummyEntity)));

		assertThat(events).containsExactly("afterSave");
	}

	@Test // DATAJDBC-454
	public void beforeDelete() {

		listener.onApplicationEvent(
				new BeforeDeleteEvent<>(Identifier.of(23), dummyEntity, MutableAggregateChange.forDelete(dummyEntity)));

		assertThat(events).containsExactly("beforeDelete");
	}

	@Test // DATAJDBC-454
	public void afterDelete() {

		listener.onApplicationEvent(
				new AfterDeleteEvent<>(Identifier.of(23), dummyEntity, MutableAggregateChange.forDelete(dummyEntity)));

		assertThat(events).containsExactly("afterDelete");
	}

	@Test // DATAJDBC-454
	public void eventWithNonMatchingDomainType() {

		String notADummyEntity = "I'm not a dummy entity";

		listener.onApplicationEvent(
				new AfterDeleteEvent<>(Identifier.of(23), String.class, MutableAggregateChange.forDelete(notADummyEntity)));

		assertThat(events).isEmpty();
	}

	static class DummyEntity {

	}

	private class EventListenerUnderTest extends AbstractRelationalEventListener<DummyEntity> {

		@Override
		protected void onBeforeConvert(BeforeConvertEvent<DummyEntity> event) {
			events.add("beforeConvert");
		}

		@Override
		protected void onBeforeSave(BeforeSaveEvent<DummyEntity> event) {
			events.add("beforeSave");
		}

		@Override
		protected void onAfterSave(AfterSaveEvent<DummyEntity> event) {
			events.add("afterSave");
		}

		@Override
		protected void onAfterConvert(AfterConvertEvent<DummyEntity> event) {
			events.add("afterConvert");
		}

		@Override
		protected void onAfterDelete(AfterDeleteEvent<DummyEntity> event) {
			events.add("afterDelete");
		}

		@Override
		protected void onBeforeDelete(BeforeDeleteEvent<DummyEntity> event) {
			events.add("beforeDelete");
		}
	}
}
