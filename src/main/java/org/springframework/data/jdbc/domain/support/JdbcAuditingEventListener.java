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

import org.springframework.beans.factory.ObjectFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.data.auditing.AuditingHandler;
import org.springframework.data.jdbc.mapping.event.BeforeSaveEvent;
import org.springframework.data.jdbc.repository.config.EnableJdbcAuditing;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Spring JDBC event listener to capture auditing information on persisting and updating entities.
 * <p>
 * An instance of this class gets registered when you apply {@link EnableJdbcAuditing} to your Spring config.
 *
 * @author Kazuki Shimizu
 * @see EnableJdbcAuditing
 * @since 1.0
 */
public class JdbcAuditingEventListener implements ApplicationListener<BeforeSaveEvent> {

	@Nullable private AuditingHandler handler;

	/**
	 * Configures the {@link AuditingHandler} to be used to set the current auditor on the domain types touched.
	 *
	 * @param auditingHandler must not be {@literal null}.
	 */
	public void setAuditingHandler(ObjectFactory<AuditingHandler> auditingHandler) {

		Assert.notNull(auditingHandler, "AuditingHandler must not be null!");

		this.handler = auditingHandler.getObject();
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @param event a notification event for indicating before save
	 */
	@Override
	public void onApplicationEvent(BeforeSaveEvent event) {

		if (handler != null) {

			event.getOptionalEntity().ifPresent(entity -> {

				if (event.getId().getOptionalValue().isPresent()) {
					handler.markModified(entity);
				} else {
					handler.markCreated(entity);
				}
			});
		}
	}
}
