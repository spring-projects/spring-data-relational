package org.springframework.data.relational.core.query.template;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

/**
 * Dynamic sql template provider
 *
 * @author kfyty725
 * @date 2023/10/21 18:31
 * @email kfyty725@hotmail.com
 */
public interface DynamicTemplateProvider<TS extends TemplateStatement> {
    /**
     * Resolve the dynamic SQL template set based on the given path
     *
     * @param paths xml template path
     * @return TemplateStatement
     */
    List<TS> resolve(List<String> paths);

    /**
     * Resolve dynamic SQL id based on given interface method
     *
     * @param method DAO interface method
     * @return id
     */
    String resolveTemplateStatementId(Method method);

    /**
     * Renders the given dynamic SQL template
     *
     * @param statementId template statement id
     * @param params      template parameters
     * @return Rendered SQL
     */
    String renderTemplate(String statementId, Map<String, Object> params);
}
