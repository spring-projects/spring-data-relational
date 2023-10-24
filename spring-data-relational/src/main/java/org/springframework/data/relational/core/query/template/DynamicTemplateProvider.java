/*
 * Copyright 2018-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
     * set template paths
     *
     * @param paths template paths
     */
    void setTemplatePath(List<String> paths);

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
