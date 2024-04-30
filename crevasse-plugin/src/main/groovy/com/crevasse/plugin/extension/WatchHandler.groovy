package com.crevasse.plugin.extension

import org.gradle.api.Named
import org.gradle.api.model.ObjectFactory
import org.gradle.api.provider.Property

import javax.inject.Inject

class WatchHandler implements Named {

    String name
    Property<String> schemaName
    Property<Boolean> enabled

    @Inject
    WatchHandler(ObjectFactory objects, String name) {
        this.name = name
        this.schemaName = objects.property(String)
        this.enabled = objects.property(Boolean)
        this.enabled.set(true)
    }

    @Override
    String getName() {
        return name
    }

    void schemaName(String schemaName) {
        this.schemaName.set(schemaName)
        this.schemaName.disallowChanges()
    }

    void disable() {
        this.enabled.set(false)
        this.enabled.disallowChanges()
    }
}
