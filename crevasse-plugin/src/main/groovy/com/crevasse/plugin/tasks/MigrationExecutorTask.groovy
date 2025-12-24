package com.crevasse.plugin.tasks

import com.crevasse.iceberg.MigrationExecutionResult
import com.crevasse.iceberg.MigrationExecutor
import com.crevasse.plugin.extension.DataFormatHandler
import com.crevasse.plugin.extension.iceberg.IcebergHandler
import org.apache.iceberg.catalog.TableIdentifier
import org.gradle.api.DefaultTask
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.provider.ListProperty
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.TaskAction

import static com.crevasse.plugin.extension.DataFormatHandler.DataFormatType.ICEBERG

class MigrationExecutorTask extends DefaultTask {

    // ANSI color codes
    private static final String RESET = "\u001B[0m"
    private static final String GREEN = "\u001B[32m"
    private static final String YELLOW = "\u001B[33m"
    private static final String CYAN = "\u001B[36m"
    private static final String BLUE = "\u001B[34m"
    private static final String BOLD = "\u001B[1m"
    private static final String DIM = "\u001B[2m"

    @Internal
    final DirectoryProperty scriptDir = project.objects.directoryProperty()

    @Input
    final ListProperty<DataFormatHandler> dataFormatHandlers = project.objects.listProperty(DataFormatHandler)

    @TaskAction
    void execute() {
        println ""
        println "${BOLD}${CYAN}Crevasse Migration Executor${RESET}"
        println "${DIM}${'─' * 50}${RESET}"

        def scriptDirFile = scriptDir.get().asFile
        if (!scriptDirFile.exists()) {
            println ""
            println "${YELLOW}⚠${RESET}  ${BOLD}No migrations directory found${RESET}"
            println "   ${DIM}Expected:${RESET} ${scriptDirFile.absolutePath}"
            println ""
            println "   Run ${CYAN}./gradlew generateMigrationScripts${RESET} to create migrations"
            println "   from your Avro schemas first."
            println ""
            println "${DIM}${'─' * 50}${RESET}"
            println "${YELLOW}No migrations to apply${RESET}"
            println ""
            return
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
                                results << it.run()
                            }
                    break
                default:
                    throw new IllegalArgumentException("Unsupported data format: ${it.dataFormatType}")
            }
        }

        printResults(results)
    }

    private void printResults(List<MigrationExecutionResult> results) {
        def applied = results.findAll { it.status == MigrationExecutionResult.Status.APPLIED }
        def dryRun = results.findAll { it.status == MigrationExecutionResult.Status.DRY_RUN }
        def skippedNoPending = results.findAll { it.status == MigrationExecutionResult.Status.SKIPPED_NO_PENDING }
        def skippedNoScripts = results.findAll { it.status == MigrationExecutionResult.Status.SKIPPED_NO_SCRIPTS }

        println ""

        // Print applied migrations
        applied.each { result ->
            println "${GREEN}✔${RESET} ${BOLD}${result.database}.${result.table}${RESET}"
            println "  ${DIM}Applied:${RESET} ${result.appliedCount} migration(s) ${DIM}(step ${result.fromStep} → ${result.toStep})${RESET}"
            if (result.appliedDescriptions) {
                result.appliedDescriptions.eachWithIndex { desc, idx ->
                    if (desc) {
                        println "  ${CYAN}${result.fromStep + idx}:${RESET} ${desc}"
                    }
                }
            }
            println ""
        }

        // Print dry run results
        dryRun.each { result ->
            println "${BLUE}◐${RESET} ${BOLD}${result.database}.${result.table}${RESET} ${DIM}(dry run)${RESET}"
            println "  ${DIM}Would apply:${RESET} ${result.appliedCount} migration(s) ${DIM}(step ${result.fromStep} → ${result.toStep})${RESET}"
            if (result.appliedDescriptions) {
                result.appliedDescriptions.eachWithIndex { desc, idx ->
                    if (desc) {
                        println "  ${CYAN}${result.fromStep + idx}:${RESET} ${desc}"
                    }
                }
            }
            println ""
        }

        // Print skipped tables (no pending)
        skippedNoPending.each { result ->
            println "${YELLOW}○${RESET} ${BOLD}${result.database}.${result.table}${RESET} ${DIM}(up to date)${RESET}"
        }

        // Print skipped tables (no scripts)
        skippedNoScripts.each { result ->
            println "${YELLOW}○${RESET} ${BOLD}${result.database}.${result.table}${RESET} ${DIM}(no scripts found)${RESET}"
        }

        // Print summary
        println ""
        println "${DIM}${'─' * 50}${RESET}"

        def totalApplied = applied.sum { it.appliedCount } ?: 0
        def totalDryRun = dryRun.sum { it.appliedCount } ?: 0

        if (totalApplied > 0) {
            println "${GREEN}${BOLD}${totalApplied} migration(s) applied to ${applied.size()} table(s)${RESET}"
        } else if (totalDryRun > 0) {
            println "${BLUE}${BOLD}${totalDryRun} migration(s) would be applied to ${dryRun.size()} table(s) (dry run)${RESET}"
        } else {
            println "${YELLOW}All tables are up to date${RESET}"
        }
        println ""
    }
}
