package org.springframework.data.relational.core.sql.render;

import org.springframework.data.relational.core.sql.Column;
import org.springframework.data.relational.core.sql.Expression;
import org.springframework.data.relational.core.sql.Select;

public class CustomSqlRenderer extends SqlRenderer {
    public CustomSqlRenderer(RenderContext context) {
        super(context);
    }

    static class CustomExpressionVisitor extends ExpressionVisitor {

        CustomExpressionVisitor(RenderContext context) {
            super(context);
        }

        @Override
        Delegation enterMatched(Expression segment) {
            if (segment instanceof Column) {

                Column column = (Column) segment;

                value = aliasHandling == AliasHandling.USE ? NameRenderer.fullyQualifiedReference(context, column)
                        : column.getName().toString();
                return Delegation.retain();
            }
            else {
                return super.enterMatched(segment);
            }
        }
    }

    static class CustomSelectListVisitor extends SelectListVisitor {

        CustomSelectListVisitor(RenderContext context, RenderTarget target) {
            super(context, target, new CustomExpressionVisitor(context));
        }
    }

    static class CustomSelectStatementVisitor extends SelectStatementVisitor {

        CustomSelectStatementVisitor(RenderContext context) {
            super(context);
            this.selectListVisitor = new CustomSelectListVisitor(context, selectList::append);
            this.orderByClauseVisitor = new OrderByClauseVisitor(context);
            this.fromClauseVisitor = new FromClauseVisitor(context, it -> {

                if (from.length() != 0) {
                    from.append(", ");
                }

                from.append(it);
            });

            this.whereClauseVisitor = new WhereClauseVisitor(context, where::append);
        }
    }

    @Override
    public String render(Select select) {
        SelectStatementVisitor visitor = new CustomSelectStatementVisitor(context);
        select.visit(visitor);

        return visitor.getRenderedPart().toString();
    }
}
