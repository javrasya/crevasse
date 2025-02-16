package com.crevasse.plugin.tasks

import com.crevasse.iceberg.MigrationExecutor
import com.crevasse.plugin.extension.DataFormatHandler
import com.crevasse.plugin.extension.iceberg.IcebergHandler
import org.apache.iceberg.catalog.TableIdentifier
import org.gradle.api.DefaultTask
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.provider.ListProperty
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.TaskAction

import static com.crevasse.plugin.extension.DataFormatHandler.DataFormatType.ICEBERG

class MigrationExecutorTask extends DefaultTask {

    @InputDirectory
    final DirectoryProperty scriptDir = project.objects.directoryProperty()

    @Input
    final ListProperty<DataFormatHandler> dataFormatHandlers = project.objects.listProperty(DataFormatHandler)

    //    @Classpath
    //    final ConfigurableFileCollection classpath = project.objects.fileCollection() // Initialize the collection
    //            .from(project.sourceSets.main.runtimeClasspath) // Set it here
    //            .from({
    //                project.configurations.crevasse.resolve()
    //            })

    @TaskAction
    void execute() {
        dataFormatHandlers.get().forEach {
            switch (it.dataFormatType) {
                case ICEBERG:
                    def icebergHandler = it as IcebergHandler
                    icebergHandler.catalogHandlers
                            .asMap
                            .values()
                            .collectMany { catalogHandler ->
                                catalogHandler.schemaHandlers.asMap.values().collect {
                                    def (db, table) = it.table.get().split("\\.")
                                    return MigrationExecutor
                                            .builder()
                                            .scriptDir(scriptDir.get().asFile.toPath())
                                            .tableIdentifier(TableIdentifier.of(db, table))
                                            .catalogSupplier(catalogHandler::getCatalog)
                                            .build()
                                }
                            }
                            .forEach {
                                it.run()
                            }
                    break
                default:
                    throw new IllegalArgumentException("Unsupported data format: ${it.dataFormatType}")
            }
        }
    }

}
