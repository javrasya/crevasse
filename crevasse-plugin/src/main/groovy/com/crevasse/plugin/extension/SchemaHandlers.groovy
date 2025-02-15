package com.crevasse.plugin.extension

import org.gradle.api.Named
import org.gradle.api.model.ObjectFactory
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.Property

import javax.inject.Inject

class SchemaHandlers implements Named {

    private String name
    Property<String> table
    Property<String> schemaName
    Property<Boolean> enabled
    ListProperty<String> ignoredColumns

    @Inject
    SchemaHandlers(ObjectFactory objects, String name) {
        this.name = name
        this.table = objects.property(String)
        this.schemaName = objects.property(String)
        this.enabled = objects.property(Boolean)
        this.enabled.set(true)
        this.ignoredColumns = objects.listProperty(String)
    }

    @Override
    String getName() {
        return name
    }

    void table(String table) {
        this.table.set(table)
        this.table.disallowChanges()
    }

    void schemaName(String schemaName) {
        this.schemaName.set(schemaName)
        this.schemaName.disallowChanges()
    }

    void disable() {
        this.enabled.set(false)
        this.enabled.disallowChanges()
    }

    void ignoredColumns(String... ignoredColumns) {
        this.ignoredColumns.addAll(ignoredColumns)
    }

    void ignoreColumn(String ignoredColumn) {
        this.ignoredColumns.add(ignoredColumn)
    }
}
