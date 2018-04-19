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
package org.springframework.data.jdbc.domain.support;

import lombok.RequiredArgsConstructor;

import org.springframework.context.ApplicationListener;
import org.springframework.data.auditing.AuditingHandler;
import org.springframework.data.jdbc.core.mapping.event.BeforeSaveEvent;
import org.springframework.data.jdbc.mapping.model.JdbcMappingContext;
import org.springframework.data.jdbc.mapping.model.JdbcPersistentEntityInformation;
import org.springframework.data.jdbc.repository.config.EnableJdbcAuditing;

/**
 * Spring JDBC event listener to capture auditing information on persisting and updating entities.
 * <p>
 * An instance of this class gets registered when you apply {@link EnableJdbcAuditing} to your Spring config.
 *
 * @author Kazuki Shimizu
 * @author Jens Schauder
 * @see EnableJdbcAuditing
 * @since 1.0
 */
@RequiredArgsConstructor
public class JdbcAuditingEventListener implements ApplicationListener<BeforeSaveEvent> {

	private final AuditingHandler handler;
	private final JdbcMappingContext context;

	/**
	 * {@inheritDoc}
	 * 
	 * @param event a notification event for indicating before save
	 */
	@Override
	public void onApplicationEvent(BeforeSaveEvent event) {

		Object entity = event.getEntity();

		@SuppressWarnings("unchecked")
		Class<Object> entityType = event.getChange().getEntityType();
		JdbcPersistentEntityInformation<Object, ?> entityInformation = context
				.getRequiredPersistentEntityInformation(entityType);

		invokeHandler(entity, entityInformation);
	}

	private <T> void invokeHandler(T entity, JdbcPersistentEntityInformation<T, ?> entityInformation) {

		if (entityInformation.isNew(entity)) {
			handler.markCreated(entity);
		} else {
			handler.markModified(entity);
		}
	}
}
