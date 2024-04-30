package com.crevasse.plugin.extension

import org.gradle.api.Action
import org.gradle.api.NamedDomainObjectContainer
import org.gradle.api.model.ObjectFactory

import javax.inject.Inject

class MigrateHandler {

    private CatalogHandler catalogHandler;

    NamedDomainObjectContainer<WatchHandler> watchedSchemas;
    private final ObjectFactory objects

    @Inject
    MigrateHandler(ObjectFactory objects) {
        this.objects = objects
        watchedSchemas = objects.domainObjectContainer(WatchHandler)
    }

    void catalog(Action<CatalogHandler> action) {
        catalogHandler = objects.newInstance(CatalogHandler)
        action.execute(catalogHandler)
    }

    void watch(Action<NamedDomainObjectContainer<WatchHandler>> action) {
        action.execute(watchedSchemas)
    }
}
