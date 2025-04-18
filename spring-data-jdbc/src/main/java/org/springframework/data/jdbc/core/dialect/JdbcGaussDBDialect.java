/*
 * Copyright 2021-2025 the original author or authors.
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
package org.springframework.data.jdbc.core.dialect;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.springframework.data.relational.core.dialect.GaussDBDialect;
import org.springframework.util.ClassUtils;

/**
 * JDBC specific GaussDBDialect Dialect.
 *
 * Notes: this file is token from JdbcPostgresDialect and add specific changes for GaussDB
 *
 * @author liubao
 */
public class JdbcGaussDBDialect extends GaussDBDialect {

    public static final JdbcGaussDBDialect INSTANCE = new JdbcGaussDBDialect();

    private static final Set<Class<?>> SIMPLE_TYPES;

    static {

        Set<Class<?>> simpleTypes = new HashSet<>(GaussDBDialect.INSTANCE.simpleTypes());
        List<String> simpleTypeNames = Arrays.asList( //
            "com.huawei.gaussdb.jdbc.util.PGobject", //
            "com.huawei.gaussdb.jdbc.geometric.PGpoint", //
            "com.huawei.gaussdb.jdbc.geometric.PGbox", //
            "com.huawei.gaussdb.jdbc.geometric.PGcircle", //
            "com.huawei.gaussdb.jdbc.geometric.PGline", //
            "com.huawei.gaussdb.jdbc.geometric.PGpath", //
            "com.huawei.gaussdb.jdbc.geometric.PGpolygon", //
            "com.huawei.gaussdb.jdbc.geometric.PGlseg" //
        );
        simpleTypeNames.forEach(name -> ifClassPresent(name, simpleTypes::add));
        SIMPLE_TYPES = Collections.unmodifiableSet(simpleTypes);
    }

    @Override
    public Set<Class<?>> simpleTypes() {
        return SIMPLE_TYPES;
    }

    /**
     * If the class is present on the class path, invoke the specified consumer {@code action} with the class object,
     * otherwise do nothing.
     *
     * @param action block to be executed if a value is present.
     */
    private static void ifClassPresent(String className, Consumer<Class<?>> action) {
        if (ClassUtils.isPresent(className, GaussDBDialect.class.getClassLoader())) {
            action.accept(ClassUtils.resolveClassName(className, GaussDBDialect.class.getClassLoader()));
        }
    }
}
