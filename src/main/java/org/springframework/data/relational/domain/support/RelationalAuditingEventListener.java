/*
 * Copyright 2018-2020 the original author or authors.
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
package org.springframework.data.relational.domain.support;

import lombok.RequiredArgsConstructor;

import org.springframework.context.ApplicationListener;
import org.springframework.core.Ordered;
import org.springframework.data.auditing.IsNewAwareAuditingHandler;
import org.springframework.data.jdbc.repository.config.EnableJdbcAuditing;
import org.springframework.data.relational.core.mapping.event.BeforeSaveEvent;

/**
 * Spring JDBC event listener to capture auditing information on persisting and updating entities.
 * <p>
 * An instance of this class gets registered when you enable auditing for Spring Data JDBC.
 *
 * @author Kazuki Shimizu
 * @author Jens Schauder
 * @author Oliver Gierke
 */
@RequiredArgsConstructor
public class RelationalAuditingEventListener implements ApplicationListener<BeforeSaveEvent>, Ordered {

	/**
	 * The order used for this {@link org.springframework.context.event.EventListener}. Ordering ensures that this
	 * {@link ApplicationListener} will run before other listeners without a specified priority.
	 *
	 * @see org.springframework.core.annotation.Order
	 * @see Ordered
	 */
	public static final int AUDITING_ORDER = 100;

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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.core.Ordered#getOrder()
	 */
	@Override
	public int getOrder() {
		return AUDITING_ORDER;
	}
}
