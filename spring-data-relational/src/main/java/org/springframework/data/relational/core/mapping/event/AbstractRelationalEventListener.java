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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.core.GenericTypeResolver;

/**
 * Base class to implement domain class specific {@link ApplicationListener} classes.
 *
 * @param <E>
 * @author Jens Schauder
 * @since 2.0
 */
public class AbstractRelationalEventListener<E> implements ApplicationListener<AbstractRelationalEvent<?>> {

	private static final Log LOG = LogFactory.getLog(AbstractRelationalEventListener.class);

	private final Class<?> domainClass;

	/**
	 * Creates a new {@link AbstractRelationalEventListener}.
	 */
	public AbstractRelationalEventListener() {

		Class<?> typeArgument = GenericTypeResolver.resolveTypeArgument(this.getClass(),
				AbstractRelationalEventListener.class);
		this.domainClass = typeArgument == null ? Object.class : typeArgument;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void onApplicationEvent(AbstractRelationalEvent<?> event) {

		if (!domainClass.isAssignableFrom(event.getType())) {
			return;
		}

		if (event instanceof AfterConvertEvent) {
			onAfterConvert((AfterConvertEvent<E>) event);
		} else if (event instanceof AfterDeleteEvent) {
			onAfterDelete((AfterDeleteEvent<E>) event);
		} else if (event instanceof AfterSaveEvent) {
			onAfterSave((AfterSaveEvent<E>) event);
		} else if (event instanceof BeforeConvertEvent) {
			onBeforeConvert((BeforeConvertEvent<E>) event);
		} else if (event instanceof BeforeDeleteEvent) {
			onBeforeDelete((BeforeDeleteEvent<E>) event);
		} else if (event instanceof BeforeSaveEvent) {
			onBeforeSave((BeforeSaveEvent<E>) event);
		}
	}

	/**
	 * Captures {@link BeforeConvertEvent}.
	 *
	 * @param event never {@literal null}.
	 */
	protected void onBeforeConvert(BeforeConvertEvent<E> event) {

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("onBeforeConvert(%s)", event.getEntity()));
		}
	}

	/**
	 * Captures {@link BeforeSaveEvent}.
	 *
	 * @param event will never be {@literal null}.
	 */
	protected void onBeforeSave(BeforeSaveEvent<E> event) {

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("onBeforeSave(%s)", event.getAggregateChange()));
		}
	}

	/**
	 * Captures {@link AfterSaveEvent}.
	 *
	 * @param event will never be {@literal null}.
	 */
	protected void onAfterSave(AfterSaveEvent<E> event) {

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("onAfterSave(%s)", event.getAggregateChange()));
		}
	}

	/**
	 * Captures {@link AfterConvertEvent}.
	 *
	 * @param event will never be {@literal null}.
	 * @since 2.3
	 */
	protected void onAfterConvert(AfterConvertEvent<E> event) {

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("onAfterConvert(%s)", event.getEntity()));
		}
	}

	/**
	 * Captures {@link AfterDeleteEvent}.
	 *
	 * @param event will never be {@literal null}.
	 */
	protected void onAfterDelete(AfterDeleteEvent<E> event) {

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("onAfterDelete(%s)", event.getAggregateChange()));
		}
	}

	/**
	 * Capture {@link BeforeDeleteEvent}.
	 *
	 * @param event will never be {@literal null}.
	 */
	protected void onBeforeDelete(BeforeDeleteEvent<E> event) {

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("onBeforeDelete(%s)", event.getAggregateChange()));
		}
	}

}
