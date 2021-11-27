package org.springframework.data.relational.core.sql.render;

import org.springframework.data.relational.core.dialect.InsertWithDefaultValues;
import org.springframework.data.relational.core.sql.Insert;

/**
 * This interface encapsulates the details about how to
 * process {@link Insert} SQL statement
 *
 * @see RenderContext
 * @author Mikhail Polivakha
 */
public interface InsertRenderContext {

    default InsertWithDefaultValues getInsertWithDefaultValues() {
        return new InsertWithDefaultValues() {};
    }
}