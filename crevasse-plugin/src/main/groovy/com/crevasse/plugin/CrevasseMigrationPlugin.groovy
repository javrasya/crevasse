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
            project.configurations {
                crevasse.extendsFrom implementation
            }

            def pluginVersion = retrievePluginVersion(project) ?: '1.0-SNAPSHOT'

            project.dependencies {
                crevasse "org.apache.avro:avro:1.11.1"

                if (extension.dataFormatHandlers.any { it.dataFormatType.name() == "ICEBERG" }) {
                    crevasse "com.crevasse:crevasse-iceberg:${pluginVersion}"
                }
            }

            project.sourceSets {
                migrations {
                    java.srcDirs = ['migrations']
                    compileClasspath += project.configurations.crevasse
                    runtimeClasspath += project.configurations.crevasse
                }
            }

//            project.sourceSets.migrations.java.srcDir new File(project.projectDir, extension.scriptDir.get().asFile.toString())



            project.tasks.register("applyMigrations", MigrationExecutorTask) {
                group = "migration"
                description = "Execute crevasse migrations."
                dependsOn project.classes

                scriptDir.set(extension.scriptDir)
                dataFormatHandlers.set(extension.dataFormatHandlers)
            }

            project.tasks.register("generateMigrationScripts", MigrationScriptGeneratorTask) {
                group = "migration"
                description = "Generate crevasse migration scripts automatically by detecting the schema changes."
                dependsOn project.classes

                classpaths = project.files(project.sourceSets.main.runtimeClasspath)
                scriptDir.set(extension.scriptDir)
                dataFormatHandlers.set(extension.dataFormatHandlers)
            }

        }
    }

    static def retrievePluginVersion(Project project) {
        def pluginArtifact = findPluginArtifact(project)
        return pluginArtifact?.version
    }

    static def findPluginArtifact(Project project) {
        def collect = project.buildscript.configurations.classpath.resolvedConfiguration.resolvedArtifacts
                .collect { it.moduleVersion.id }

        def pluginArtifact = collect
                .find { it.group == 'com.crevasse' && it.name == 'crevasse-plugin' }

        if (pluginArtifact != null || project.parent == null) {
            return pluginArtifact
        }

        return findPluginArtifact(project.parent)
    }
}