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
