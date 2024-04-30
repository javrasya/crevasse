package com.crevasse.plugin.extension

import org.gradle.api.Action
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.model.ObjectFactory
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.Property

import javax.inject.Inject

class RootExtension {
    DirectoryProperty scriptDir;

    ListProperty<MigrateHandler> migrateHandlers;

    private ObjectFactory objects

    @Inject
    RootExtension(ObjectFactory objects) {
        this.objects = objects
        scriptDir = objects.directoryProperty()
        migrateHandlers = objects.listProperty(MigrateHandler)
    }

    void migrate(Action<MigrateHandler> action) {
        MigrateHandler newMigrateHandler = objects.newInstance(MigrateHandler)
        action.execute(newMigrateHandler)
        migrateHandlers.add(newMigrateHandler)
    }
}
