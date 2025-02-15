package com.crevasse.plugin.extension.iceberg

import com.crevasse.iceberg.catalog.CatalogLoader
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.catalog.Catalog
import org.gradle.api.model.ObjectFactory
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Optional

import javax.inject.Inject

class GlueCatalogHandler extends CatalogHandler {

    @Optional
    Property<String> ioImpl

    @Inject
    GlueCatalogHandler(ObjectFactory objects, String name) {
        super(objects, name)
        this.ioImpl = objects.property(String)
        ioImpl.set("org.apache.iceberg.aws.s3.S3FileIO")
    }

    void ioImpl(String ioImpl) {
        this.ioImpl.set(ioImpl)
        this.ioImpl.disallowChanges()
    }

    @Override
    Catalog getCatalog() {
        return CatalogLoader.custom(
                "glue",
                [
                        "warehouse": warehouse.get(),
                        "io-impl"  : ioImpl.get()
                ],
                new Configuration(),
                "org.apache.iceberg.aws.glue.GlueCatalog"
        ).loadCatalog()
    }
}
