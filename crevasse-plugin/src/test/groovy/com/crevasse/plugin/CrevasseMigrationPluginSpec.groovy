package com.crevasse.plugin

import org.gradle.testkit.runner.GradleRunner
import org.gradle.util.GradleVersion
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Paths

class CrevasseMigrationPluginSpec extends Specification {


    @TempDir
    File testProjectRootDir

    File buildFile
    File settingsFile

    def setup() {
        println "Testing using Gradle version ${gradleVersion}."

        buildFile = projectFile("build.gradle")

        settingsFile = projectFile("settings.gradle")

        settingsFile << "rootProject.name = 'test-project'\n"
    }

    def "should add the gradle task to execute the migrations"() {
        given:

        buildFile << """
            plugins {
                id 'com.crevasse.plugin'
            }
        """

        addDefaultRepository()

        buildFile << """
            crevasse {
                scriptDir = file("src/main/resources/migrations/")
                migrate {
                    catalog {
                        providerImpl "com.crevasse.SomeCatalogProvider"
                        arguments([
                            "key": "value"
                        ])
                    }
                }
            }
        """

        when:
        def result = GradleRunner.create()
                .withProjectDir(testProjectRootDir)
                .withArguments("tasks")
                .withPluginClasspath()
                .build()

        then:
        result.output.contains("executeMigrations")
        !result.output.contains("generateMigrationScripts")
    }


    def "should add the gradle task to generate the migrations when watch is configured"() {
        given:

        buildFile << """
            plugins {
                id 'com.crevasse.plugin'
            }
        """

        addDefaultRepository()

        buildFile << """
            crevasse {
                scriptDir = file("src/main/resources/migrations/")
                migrate {
                    catalog {
                        providerImpl "com.crevasse.SomeCatalogProvider"
                        arguments([
                            "key": "value"
                        ])
                    }
                    watch {
                        mySchema1 {
                            schemaName "com.crevasse.MySchema1"
                        }
                    }
                }
            }
        """

        when:
        def result = GradleRunner.create()
                .withProjectDir(testProjectRootDir)
                .withArguments("tasks")
                .withPluginClasspath()
                .build()

        then:
        result.output.contains("executeMigrations")
        result.output.contains("generateMigrationScripts")
    }


    protected File projectFile(String path) {
        File file = new File(Paths.get(testProjectRootDir.toPath().toString(), path).toString())
        file.parentFile.mkdirs()
        return file
    }

    protected static GradleVersion getGradleVersion() {
        def version = System.getProperty("gradleVersion")
        if (!version) {
            throw new IllegalArgumentException("gradleVersion project property is required")
        }
        return GradleVersion.version(version)
    }

    protected void addDefaultRepository() {
        buildFile << "repositories { mavenCentral() }\n"
    }

}
