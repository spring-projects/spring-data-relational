package org.springframework.data.relational.core.mapping.event;

import org.springframework.data.mapping.callback.EntityCallback;

/**
 * Represents {@link EntityCallback}, that will be triggered after
 * particular DML query (e.g. SELECT, INSERT, DELETE)
 *
 * Examples are:
 * @see AfterDeleteCallback
 * @see AfterSaveCallback
 * @see AfterLoadCallback
 *
 * @author Mikhail Polivakha
 */
public interface EntityCallbackAfter<T> extends EntityCallback<T> {

}