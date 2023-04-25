package org.springframework.data.relational.core.mapping.schemasqlgeneration.liquibase;

import java.util.List;

public class ChangeSet {
    private String id;
    private String author;

    class ChangeSetChange {
        private String type; // createTable, addColumn, dropTable, dropColumn
        private String tableName;
        private List<ChangeSetColumns> columns;
    }

    class ChangeSetColumns {
        private String name;
        private String type;
    }

}
