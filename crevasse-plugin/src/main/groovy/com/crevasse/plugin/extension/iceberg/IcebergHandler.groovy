package com.crevasse.plugin.extension.iceberg

import com.crevasse.plugin.extension.DataFormatHandler
import org.gradle.api.Action
import org.gradle.api.PolymorphicDomainObjectContainer
import org.gradle.api.model.ObjectFactory

import javax.inject.Inject

class IcebergHandler implements DataFormatHandler {

    private transient PolymorphicDomainObjectContainer<CatalogHandler> catalogHandlers;

    @Inject
    IcebergHandler(ObjectFactory objects) {
        catalogHandlers = objects.polymorphicDomainObjectContainer(CatalogHandler)

        catalogHandlers.registerBinding(GlueCatalogHandler, GlueCatalogHandler)
        catalogHandlers.register("glue", GlueCatalogHandler)

        catalogHandlers.registerBinding(HadoopCatalogHandler, HadoopCatalogHandler)
        catalogHandlers.register("hadoop", HadoopCatalogHandler)

    }

    void catalogs(Action<PolymorphicDomainObjectContainer<CatalogHandler>> action) {
        action.execute(catalogHandlers)
    }

    @Override
    DataFormatType getDataFormatType() {
        return DataFormatType.ICEBERG
    }

    PolymorphicDomainObjectContainer<CatalogHandler> getCatalogHandlers() {
        return catalogHandlers
    }
}
