package org.springframework.data.relational.core.query.template;

/**
 * Dynamic sql template statement
 *
 * @author kfyty725
 * @date 2023/10/21 18:06
 * @email kfyty725@hotmail.com
 */
public abstract class TemplateStatement {
    /**
     * name of dynamic template namespace in xml attribute
     */
    public static final String TEMPLATE_NAMESPACE = "namespace";

    /**
     * name of dynamic template statement id in xml attribute
     */
    public static final String TEMPLATE_STATEMENT_ID = "id";

    /**
     * template statement type: select
     */
    public static final String SELECT_LABEL = "select";

    /**
     * template statement type: execute
     */
    public static final String EXECUTE_LABEL = "execute";

    /**
     * dynamic template statement id.
     * namespace and id is unique identifier of dynamic sql template statement.
     */
    private String id;

    /**
     * template statement type
     *
     * @see TemplateStatement#SELECT_LABEL
     * @see TemplateStatement#EXECUTE_LABEL
     */
    private String labelType;

    public TemplateStatement(String id, String labelType) {
        this.id = id;
        this.labelType = labelType;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getLabelType() {
        return labelType;
    }

    public void setLabelType(String labelType) {
        this.labelType = labelType;
    }
}
