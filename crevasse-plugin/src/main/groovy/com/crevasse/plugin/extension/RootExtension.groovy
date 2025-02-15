package com.crevasse.plugin.extension

import com.crevasse.plugin.extension.iceberg.IcebergHandler
import org.gradle.api.Action
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.model.ObjectFactory
import org.gradle.api.provider.Property

import javax.inject.Inject

class RootExtension {
    DirectoryProperty scriptDir;

    Property<IcebergHandler> icebergHandler;

    private ObjectFactory objects

    @Inject
    RootExtension(ObjectFactory objects) {
        this.objects = objects
        scriptDir = objects.directoryProperty()
        icebergHandler = objects.property(IcebergHandler)
    }

    void iceberg(Action<IcebergHandler> action) {
        def newIcebergHandler = objects.newInstance(IcebergHandler)
        action.execute(newIcebergHandler)
        icebergHandler.set(newIcebergHandler)
        icebergHandler.disallowChanges()
    }

    List<DataFormatHandler> getDataFormatHandlers() {
        return [icebergHandler.get()]
    }
}
