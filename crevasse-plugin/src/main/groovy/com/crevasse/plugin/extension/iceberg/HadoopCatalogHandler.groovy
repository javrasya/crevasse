package com.crevasse.plugin.extension.iceberg

import com.crevasse.iceberg.catalog.CatalogLoader
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.catalog.Catalog
import org.gradle.api.model.ObjectFactory

import javax.inject.Inject

class HadoopCatalogHandler extends CatalogHandler {

    @Inject
    HadoopCatalogHandler(ObjectFactory objects, String name) {
        super(objects, name)
    }

    @Override
    Catalog getCatalog() {
        return CatalogLoader.hadoop(
                "hadoop",
                new Configuration(),
                [
                        "warehouse": warehouse.get()
                ]
        ).loadCatalog()
    }
}
