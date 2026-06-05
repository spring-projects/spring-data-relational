/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.jdbc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.context.ApplicationListener;
import org.springframework.data.relational.core.mapping.event.AbstractRelationalEvent;

/**
 * {@link ApplicationListener} that captures {@link AbstractRelationalEvent RelationalEvents}.
 *
 * @author Christoph Strobl
 */
public class CapturingEventListener implements ApplicationListener<AbstractRelationalEvent<?>> {

	private final List<AbstractRelationalEvent<?>> events = new ArrayList<>(5);

	private volatile boolean record;

	@Override
	public void onApplicationEvent(AbstractRelationalEvent<?> event) {

		if (!isRecording()) {
			return;
		}
		this.events.add(event);
	}

	public boolean isRecording() {
		return this.record;
	}

	public void startRecording() {
		this.record = true;
	}

	public void stopRecording() {
		this.record = false;
	}

	public List<AbstractRelationalEvent<?>> getEvents() {
		return Collections.unmodifiableList(this.events);
	}

	public void clear() {
		this.events.clear();
	}

}
