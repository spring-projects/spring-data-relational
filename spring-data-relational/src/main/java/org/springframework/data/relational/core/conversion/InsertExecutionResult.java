package org.springframework.data.relational.core.conversion;

import org.springframework.lang.Nullable;

/**
 * Represents the result of the execution of the {@link DbAction.Insert} or {@link DbAction.InsertRoot} db actions.
 * Apart from db action, declared in {@link DbActionExecutionResult}, this class also encapsulates information about
 * possibly newly generated Id dring insertion into database.
 *
 * @author Mikhail Polivakha
 */
public class InsertExecutionResult extends DbActionExecutionResult {

    /**
     * Represents possibly Newly generated id during either {@link DbAction.Insert} or {@link DbAction.InsertRoot}.
     * Might be {@literal null} in case the id was not generated during insert, but instead was provided in advance
     */
    private final Object newId;

    public InsertExecutionResult(DbAction<?> action, @Nullable Object newId) {
        super(action);
        this.newId = newId;
    }

    @Nullable
    public Object getNewId() {
        return newId;
    }
}