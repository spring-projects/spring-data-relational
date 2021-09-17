package org.springframework.data.relational.core.mapping.event;

import org.springframework.data.mapping.callback.EntityCallback;

/**
 * Represents {@link EntityCallback}, that will be triggered before
 * particular DML query (e.g. SELECT, INSERT, DELETE)
 *
 * Examples are:
 * @see BeforeDeleteCallback
 * @see BeforeSaveCallback
 * @see BeforeConvertCallback
 *
 * @author Mikhail Polivakha
 */
public interface EntityCallbackBefore<T> extends EntityCallback<T> {

}