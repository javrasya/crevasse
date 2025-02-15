package com.crevasse.plugin.tasks

import com.crevasse.iceberg.MigrationScriptGenerator
import com.crevasse.plugin.extension.DataFormatHandler
import com.crevasse.plugin.extension.iceberg.IcebergHandler
import com.crevasse.plugin.utils.ReflectionUtils
import org.apache.iceberg.catalog.TableIdentifier
import org.gradle.api.DefaultTask
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.provider.ListProperty
import org.gradle.api.tasks.*

import java.nio.file.Path
import java.nio.file.Paths

import static com.crevasse.plugin.extension.DataFormatHandler.DataFormatType.ICEBERG
import static com.crevasse.plugin.utils.ReflectionUtils.getSchema

class MigrationScriptGeneratorTask extends DefaultTask {

    @InputDirectory
    final DirectoryProperty scriptDir = project.objects.directoryProperty()

    @Input
    final ListProperty<DataFormatHandler> dataFormatHandlers = project.objects.listProperty(DataFormatHandler)

    @InputFiles
    final ConfigurableFileCollection classpaths = project.objects.fileCollection()

    @OutputFile
    final Path serializedOutputFile = project.layout.buildDirectory.file("migration/migrationScriptGeneratorTaskDtos.ser").get().asFile.toPath()

    @TaskAction
    void execute() {
        dataFormatHandlers.get().forEach {
            switch (it.dataFormatType) {
                case ICEBERG:
                    def icebergHandler = it as IcebergHandler
                    return icebergHandler.catalogHandlers
                            .asMap
                            .values()
                            .collectMany { catalogHandler ->
                                catalogHandler.schemaHandlers.asMap.values()
                            }
                            .collect {
                                def clazz = ReflectionUtils.loadClass(it.schemaName.get(), classpaths)
                                def schema = getSchema(clazz)
                                def (db, table) = it.table.get().split("\\.")

                                return MigrationScriptGenerator.builder()
                                        .scriptDir(Paths.get(scriptDir.get().asFile.toString()))
                                        .tableIdentifier(TableIdentifier.of(db, table))
                                        .avroSchema(schema)
                                        .ignoredColumns(it.ignoredColumns.get())
                                        .build()
                            }
                            .forEach {
                                it.generateMigration()
                            }
                default:
                    throw new IllegalArgumentException("Unsupported data format: ${it.dataFormatType}")
            }
        }

//        // Serialize and save the task DTOs
//        Path serializedPath = serializeAndSave(migrationScriptGeneratorTaskDtos)
//
//        // Combine the classpath files
//        FileCollection combinedClasspath = classpaths.plus(project.files())
//
//        // Execute the Java process
//        project.javaexec {
//            classpath combinedClasspath
//            mainClass = 'com.crevasse.plugin.executables.MigrationScriptGeneratorTaskMain'
//            args serializedPath.toString()
//        }
    }
//
//    private Path serializeAndSave(List<MigrationScriptGeneratorTaskDto> migrationScriptGeneratorTaskDtos) {
//        if (!serializedOutputFile.parent.toFile().exists()) {
//            Files.createDirectories(serializedOutputFile.parent)
//        }
//        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(Files.newOutputStream(serializedOutputFile))) {
//            objectOutputStream.writeObject(migrationScriptGeneratorTaskDtos)
//        }
//        return serializedOutputFile
//    }
}
