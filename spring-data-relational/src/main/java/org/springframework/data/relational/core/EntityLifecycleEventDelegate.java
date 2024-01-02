/*
 * Copyright 2022-2024 the original author or authors.
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
package org.springframework.data.relational.core;

import java.util.function.Supplier;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.lang.Nullable;

/**
 * Delegate class to encapsulate lifecycle event configuration and publishing. Event creation is deferred within an
 * event {@link Supplier} to delay the actual event object creation.
 *
 * @author Mark Paluch
 * @since 3.0
 * @see ApplicationEventPublisher
 */
public class EntityLifecycleEventDelegate {

	private @Nullable ApplicationEventPublisher publisher;
	private boolean eventsEnabled = true;

	public void setPublisher(@Nullable ApplicationEventPublisher publisher) {
		this.publisher = publisher;
	}

	public boolean isEventsEnabled() {
		return eventsEnabled;
	}

	public void setEventsEnabled(boolean eventsEnabled) {
		this.eventsEnabled = eventsEnabled;
	}

	/**
	 * Publish an application event if event publishing is enabled.
	 *
	 * @param eventSupplier the supplier for application events.
	 */
	public void publishEvent(Supplier<?> eventSupplier) {

		if (canPublishEvent()) {
			publisher.publishEvent(eventSupplier.get());
		}
	}

	private boolean canPublishEvent() {
		return publisher != null && eventsEnabled;
	}
}
