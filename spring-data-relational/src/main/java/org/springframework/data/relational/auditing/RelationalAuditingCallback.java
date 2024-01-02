/*
 * Copyright 2018-2024 the original author or authors.
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
package org.springframework.data.relational.auditing;

import org.springframework.beans.factory.ObjectFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.core.Ordered;
import org.springframework.data.auditing.IsNewAwareAuditingHandler;
import org.springframework.data.relational.core.mapping.event.BeforeConvertCallback;
import org.springframework.util.Assert;

/**
 * {@link BeforeConvertCallback} to capture auditing information on persisting and updating entities.
 * <p>
 * An instance of this class gets registered when you enable auditing for Spring Data Relational.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @since 1.1
 */
public class RelationalAuditingCallback implements BeforeConvertCallback<Object>, Ordered {

	/**
	 * The order used for this {@link org.springframework.context.event.EventListener}. Ordering ensures that this
	 * {@link ApplicationListener} will run before other listeners without a specified priority.
	 *
	 * @see org.springframework.core.annotation.Order
	 * @see Ordered
	 */
	public static final int AUDITING_ORDER = 100;

	private final ObjectFactory<IsNewAwareAuditingHandler> auditingHandlerFactory;

	public RelationalAuditingCallback(ObjectFactory<IsNewAwareAuditingHandler> auditingHandlerFactory) {

		Assert.notNull(auditingHandlerFactory, "IsNewAwareAuditingHandler must not be null;");

		this.auditingHandlerFactory = auditingHandlerFactory;
	}

	@Override
	public int getOrder() {
		return AUDITING_ORDER;
	}

	@Override
	public Object onBeforeConvert(Object entity) {
		return auditingHandlerFactory.getObject().markAudited(entity);
	}
}
