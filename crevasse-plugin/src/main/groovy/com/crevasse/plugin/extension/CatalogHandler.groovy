package com.crevasse.plugin.extension

import org.gradle.api.model.ObjectFactory
import org.gradle.api.provider.MapProperty
import org.gradle.api.provider.Property

import javax.inject.Inject

class CatalogHandler {
    Property<String> providerImpl;
    MapProperty<String, Object> arguments;

    @Inject
    CatalogHandler(ObjectFactory objects) {
        this.providerImpl = objects.property(String)
        this.arguments = objects.mapProperty(String, Object)
    }

    void providerImpl(String providerImpl) {
        this.providerImpl.set(providerImpl)
        this.providerImpl.disallowChanges()
    }

    void arguments(Map<String, Object> arguments) {
        this.arguments.set(arguments)
        this.arguments.disallowChanges()
    }
}
