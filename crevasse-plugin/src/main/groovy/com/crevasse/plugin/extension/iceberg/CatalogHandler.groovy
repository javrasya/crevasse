package com.crevasse.plugin.extension.iceberg

import com.crevasse.plugin.extension.SchemaHandlers
import org.apache.iceberg.catalog.Catalog
import org.gradle.api.Action
import org.gradle.api.Named
import org.gradle.api.NamedDomainObjectContainer
import org.gradle.api.model.ObjectFactory
import org.gradle.api.provider.Property

abstract class CatalogHandler implements Named, Serializable {
    String namedName;

    Property<String> name;

    Property<String> warehouse

    NamedDomainObjectContainer<SchemaHandlers> schemaHandlers

    CatalogHandler(ObjectFactory objects, String name) {
        this.namedName = name
        this.name = objects.property(String)
        this.warehouse = objects.property(String)
        this.schemaHandlers = objects.domainObjectContainer(SchemaHandlers)
    }

    @Override
    String getName() {
        return this.namedName
    }

    void name(String name) {
        this.name.set(name)
        this.name.disallowChanges()
    }

    void warehouse(String warehouse) {
        this.warehouse.set(warehouse)
        this.warehouse.disallowChanges()
    }

    void schemas(Action<NamedDomainObjectContainer<SchemaHandlers>> action) {
        action.execute(schemaHandlers)
    }

    abstract Catalog getCatalog()
}
