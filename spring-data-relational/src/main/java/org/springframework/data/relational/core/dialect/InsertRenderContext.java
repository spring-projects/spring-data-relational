package org.springframework.data.relational.core.dialect;

import org.springframework.data.relational.core.sql.Insert;
import org.springframework.data.relational.core.sql.render.RenderContext;

/**
 * This interface encapsulates the details about how to process {@link Insert} SQL statement
 *
 * @see RenderContext
 * @author Mikhail Polivakha
 * @since 2.4
 */
public interface InsertRenderContext {

	String getDefaultValuesInsertPart();
}
