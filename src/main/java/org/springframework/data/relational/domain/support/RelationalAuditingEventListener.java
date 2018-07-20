/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.relational.domain.support;

import lombok.RequiredArgsConstructor;

import org.springframework.context.ApplicationListener;
import org.springframework.data.auditing.IsNewAwareAuditingHandler;
import org.springframework.data.jdbc.repository.config.EnableJdbcAuditing;
import org.springframework.data.relational.core.mapping.event.BeforeSaveEvent;

/**
 * Spring JDBC event listener to capture auditing information on persisting and updating entities.
 * <p>
 * An instance of this class gets registered when you apply {@link EnableJdbcAuditing} to your Spring config.
 *
 * @author Kazuki Shimizu
 * @author Jens Schauder
 * @author Oliver Gierke
 * @see EnableJdbcAuditing
 */
@RequiredArgsConstructor
public class RelationalAuditingEventListener implements ApplicationListener<BeforeSaveEvent> {

	private final IsNewAwareAuditingHandler handler;

	/**
	 * {@inheritDoc}
	 * 
	 * @param event a notification event for indicating before save
	 */
	@Override
	public void onApplicationEvent(BeforeSaveEvent event) {
		handler.markAudited(event.getEntity());
	}
}
