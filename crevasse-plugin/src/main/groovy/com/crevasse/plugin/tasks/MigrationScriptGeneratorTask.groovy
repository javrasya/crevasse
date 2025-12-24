package com.crevasse.plugin.tasks

import com.crevasse.iceberg.MigrationGenerationResult
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

    // ANSI color codes
    private static final String RESET = "\u001B[0m"
    private static final String GREEN = "\u001B[32m"
    private static final String YELLOW = "\u001B[33m"
    private static final String CYAN = "\u001B[36m"
    private static final String BOLD = "\u001B[1m"
    private static final String DIM = "\u001B[2m"

    @InputFiles
    @Optional
    final DirectoryProperty scriptDir = project.objects.directoryProperty()

    @Input
    final ListProperty<DataFormatHandler> dataFormatHandlers = project.objects.listProperty(DataFormatHandler)

    @InputFiles
    final ConfigurableFileCollection classpaths = project.objects.fileCollection()

    @OutputFile
    final Path serializedOutputFile = project.layout.buildDirectory.file("migration/migrationScriptGeneratorTaskDtos.ser").get().asFile.toPath()

    @TaskAction
    void execute() {
        println ""
        println "${BOLD}${CYAN}Crevasse Migration Script Generator${RESET}"
        println "${DIM}${'─' * 50}${RESET}"

        def scriptDirAsFile = scriptDir.get().asFile
        if (!scriptDirAsFile.exists()) {
            scriptDirAsFile.mkdirs()
        }

        def results = []

        dataFormatHandlers.get().forEach {
            switch (it.dataFormatType) {
                case ICEBERG:
                    def icebergHandler = it as IcebergHandler
                    icebergHandler.catalogHandlers
                            .asMap
                            .values()
                            .collectMany { catalogHandler ->
                                catalogHandler.schemaHandlers.asMap.values()
                            }
                            .each {
                                def clazz = ReflectionUtils.loadClass(it.schemaName.get(), classpaths)
                                def schema = getSchema(clazz)
                                def (db, table) = it.table.get().split("\\.")

                                def generator = MigrationScriptGenerator.builder()
                                        .scriptDir(Paths.get(scriptDir.get().asFile.toString()))
                                        .tableIdentifier(TableIdentifier.of(db, table))
                                        .avroSchema(schema)
                                        .ignoredColumns(it.ignoredColumns.get())
                                        .build()

                                results << generator.generateMigration()
                            }
                    break
                default:
                    throw new IllegalArgumentException("Unsupported data format: ${it.dataFormatType}")
            }
        }

        printResults(results)
    }

    private void printResults(List<MigrationGenerationResult> results) {
        def created = results.findAll { it.status == MigrationGenerationResult.Status.CREATED }
        def skipped = results.findAll { it.status == MigrationGenerationResult.Status.SKIPPED_NO_CHANGES }

        println ""

        // Print created migrations
        created.each { result ->
            println "${GREEN}✔${RESET} ${BOLD}${result.database}.${result.table}${RESET}"
            println "  ${DIM}Created:${RESET} migration_${result.stepNumber}.groovy"
            println "  ${DIM}Path:${RESET}    ${result.scriptPath}"
            if (result.operations) {
                println "  ${DIM}Changes:${RESET}"
                result.operations.each { op ->
                    println "    ${CYAN}•${RESET} ${op}"
                }
            }
            println ""
        }

        // Print skipped tables
        skipped.each { result ->
            println "${YELLOW}○${RESET} ${BOLD}${result.database}.${result.table}${RESET} ${DIM}(no changes)${RESET}"
        }

        // Print summary
        println ""
        println "${DIM}${'─' * 50}${RESET}"
        if (created) {
            println "${GREEN}${BOLD}${created.size()} migration(s) created${RESET}"
        } else {
            println "${YELLOW}No migrations created - schemas are up to date${RESET}"
        }
        println ""
    }
}
