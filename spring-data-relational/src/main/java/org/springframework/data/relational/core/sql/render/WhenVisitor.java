package org.springframework.data.relational.core.sql.render;

import org.springframework.data.relational.core.sql.Visitable;
import org.springframework.data.relational.core.sql.When;

public class WhenVisitor extends TypedSingleConditionRenderSupport<When> implements PartRenderer {
    private final StringBuilder part = new StringBuilder();
    private boolean conditionRendeder;

    WhenVisitor(RenderContext context) {
        super(context);
    }

    @Override
    Delegation leaveNested(Visitable segment) {

        if (hasDelegatedRendering()) {

            if (conditionRendeder) {
                part.append(" THEN ");
            }

            part.append(consumeRenderedPart());
            conditionRendeder = true;
        }

        return super.leaveNested(segment);
    }

    @Override
    Delegation enterMatched(When segment) {

        part.append("WHEN ");

        return super.enterMatched(segment);
    }

    @Override
    public CharSequence getRenderedPart() {
        return part;
    }
}
