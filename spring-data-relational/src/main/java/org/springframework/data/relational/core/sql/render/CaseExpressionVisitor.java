package org.springframework.data.relational.core.sql.render;

import org.springframework.data.relational.core.sql.CaseExpression;
import org.springframework.data.relational.core.sql.Literal;
import org.springframework.data.relational.core.sql.Visitable;
import org.springframework.data.relational.core.sql.When;

/**
 * Renderer for {@link CaseExpression}.
 *
 * @author Sven Rienstra
 * @since 3.4
 */
public class CaseExpressionVisitor extends TypedSingleConditionRenderSupport<CaseExpression> implements PartRenderer {
    private final StringBuilder part = new StringBuilder();

    CaseExpressionVisitor(RenderContext context) {
        super(context);
    }

    @Override
    Delegation leaveNested(Visitable segment) {

        if (hasDelegatedRendering()) {
            CharSequence renderedPart = consumeRenderedPart();

            if (segment instanceof When) {
                part.append(" ");
                part.append(renderedPart);
            } else if (segment instanceof Literal<?>) {
                part.append(" ELSE ");
                part.append(renderedPart);
            }
        }

        return super.leaveNested(segment);
    }

    @Override
    Delegation enterMatched(CaseExpression segment) {

        part.append("CASE");

        return super.enterMatched(segment);
    }

    @Override
    Delegation leaveMatched(CaseExpression segment) {

        part.append(" END");

        return super.leaveMatched(segment);
    }

    @Override
    public CharSequence getRenderedPart() {
        return part;
    }
}
