package com.crevasse.plugin

import com.crevasse.plugin.extension.RootExtension
import com.crevasse.plugin.tasks.MigrationExecutorTask
import com.crevasse.plugin.tasks.MigrationScriptGeneratorTask
import org.gradle.api.Plugin
import org.gradle.api.Project

class CrevasseMigrationPlugin implements Plugin<Project> {
    @Override
    void apply(Project project) {
        def extension = project.extensions.create("crevasse", RootExtension)

        project.afterEvaluate {
            project.tasks.register("executeMigrations", MigrationExecutorTask) {
                group = "migration"
                description = "Execute crevasse migrations."
            }

            if (extension.migrateHandlers.get()*.watchedSchemas.any {
                it.size() > 0
            })
                project.tasks.register("generateMigrationScripts", MigrationScriptGeneratorTask) {
                    group = "migration"
                    description = "Generate crevasse migration scripts automatically by detecting the schema changes."
                }
        }
    }
}
