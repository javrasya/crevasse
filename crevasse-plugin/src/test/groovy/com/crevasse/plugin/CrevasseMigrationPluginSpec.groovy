package com.crevasse.plugin


import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.hadoop.HadoopCatalog
import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.GradleRunner
import org.gradle.util.GradleVersion
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Path
import java.nio.file.Paths

class CrevasseMigrationPluginSpec extends Specification {


    @TempDir
    File testProjectRootDir

    @TempDir
    File scriptPath

    @TempDir
    File icebergWarehouse1

    @TempDir
    File icebergWarehouse2

    File buildFile

    File settingsFile

    def setup() {
        println "Testing using Gradle version ${gradleVersion}."

        settingsFile = projectFile("settings.gradle")

        final Path crevasseRootPath = Paths.get("./").normalize().toAbsolutePath().parent;

        settingsFile << """
        plugins {
            id 'org.gradle.toolchains.foojay-resolver-convention' version '1.0.0'
        }

        rootProject.name = 'test-project'

        includeBuild("${crevasseRootPath.toString()}/")  {
            dependencySubstitution {
                substitute module("com.crevasse:crevasse-iceberg") using project(":crevasse-iceberg")
            }
        }
        """


        buildFile = projectFile("build.gradle")

        buildFile << """
            plugins {
                id 'java'
                id 'groovy'
                id "com.github.davidmc24.gradle.plugin.avro" version "1.9.1"
                id 'com.crevasse.plugin'
            }

            repositories { mavenCentral() }

            apply plugin: "com.github.davidmc24.gradle.plugin.avro"

            java {
                toolchain {
                    languageVersion = JavaLanguageVersion.of(21)
                }
            }

            sourceSets.main.java.srcDir layout.buildDirectory.dir('generated-main-avro-java')
            sourceSets.test.java.srcDir layout.buildDirectory.dir('generated-test-avro-java')

            dependencies {
                implementation 'org.apache.avro:avro:1.11.1'
            }
        """


        Paths.get(testProjectRootDir.toString(), "src", "main", "avro").toFile().mkdirs()

        def avroIdlFile = projectFile("src/main/avro/mySchema1.avdl")

        avroIdlFile << """
            @namespace("com.crevasse")
            protocol MySchema1 {
                record MyRecord1 {
                    string name;
                    int age;
                }
            }
        """

    }

    def "should generate migration scripts under the given script path"() {
        given:


        buildFile << """
            crevasse {
                scriptDir = file("${scriptPath.toPath().toString()}")
                iceberg {
                    catalogs {
                        glue {
                            name "productionCatalog"
                            warehouse "s3://my-bucket/warehouse"
                            schemas {
                                mySchema1 {
                                    table "my_db.my_table"
                                    schemaName "com.crevasse.MyRecord1"
                                }
                            }
                        }
                    }
                }
            }
        """

        when:
        def result = run("tasks")

        then:
        result.output.contains("generateMigrationScripts")

        when:
        result = run("clean", "build", "generateMigrationScripts")

        then:
        result.output.contains("BUILD SUCCESSFUL")

        def folderToKeepDatabases = scriptPath.listFiles()
        folderToKeepDatabases.size() == 1
        folderToKeepDatabases[0].name == "my_db"

        def folderToKeepTables = folderToKeepDatabases[0].listFiles()
        folderToKeepTables.size() == 1
        folderToKeepTables[0].name == "my_table"

        def folderToKeepGeneratedMigrationScripts = folderToKeepTables[0].listFiles()
        folderToKeepGeneratedMigrationScripts.size() == 1

        def firstMigrationScript = folderToKeepGeneratedMigrationScripts[0]
        firstMigrationScript.name == "migration_0.groovy"
    }

    def "should execute the migration scripts"() {
        given:
        buildFile << """
            crevasse {
                scriptDir = file("${scriptPath.toPath().toString()}")
                iceberg {
                    catalogs {
                        hadoop {
                            name "productionCatalog1"
                            warehouse "file://${icebergWarehouse1.toString()}"
                            schemas {
                                mySchema1 {
                                    table "my_db.my_table1"
                                    schemaName "com.crevasse.MyRecord1"
                                }
                            }
                        }
                    }
                }
            }
        """

        when: "tasks listed"
        def result = run("tasks")

        then: "result contains applyMigrations task"
        result.output.contains("applyMigrations")

        when: "the task runs to generate migration scripts"
        result = run("generateMigrationScripts")

        then: "the status is successful"
        result.output.contains("BUILD SUCCESSFUL")

        when: "the task runs to apply the migration scripts (either generated or manually created)"
        result = run("applyMigrations")

        then: "the status is successful"
        result.output.contains("BUILD SUCCESSFUL")

        then: "and the table is created"
        def hadoopCatalog1 = getHadoopCatalog(icebergWarehouse1)
        def tableIdentifier1 = TableIdentifier.of("my_db", "my_table1")
        hadoopCatalog1.tableExists(tableIdentifier1)

        then: "and the table schema is correct"
        def table1 = hadoopCatalog1.loadTable(tableIdentifier1)
        table1.schema().columns().size() == 2
        table1.schema().columns().get(0).name() == "name"
        table1.schema().columns().get(1).name() == "age"
    }

    def "should handle missing migrations directory gracefully"() {
        given:
        def nonExistentScriptPath = new File(testProjectRootDir, "non_existent_migrations")

        buildFile << """
            crevasse {
                scriptDir = file("${nonExistentScriptPath.toPath().toString()}")
                iceberg {
                    catalogs {
                        hadoop {
                            name "productionCatalog1"
                            warehouse "file://${icebergWarehouse1.toString()}"
                            schemas {
                                mySchema1 {
                                    table "my_db.my_table1"
                                    schemaName "com.crevasse.MyRecord1"
                                }
                            }
                        }
                    }
                }
            }
        """

        when: "the task runs to apply migrations with non-existent directory"
        def result = run("applyMigrations")

        then: "the build succeeds with a warning message"
        result.output.contains("BUILD SUCCESSFUL")
        result.output.contains("No migrations directory found")
        result.output.contains("generateMigrationScripts")
    }

    protected BuildResult run(String... args = ["clean", "build", "-x", "test"]) {
        return createGradleRunner().withArguments(determineGradleArguments(args)).build()
    }

    private static List<String> determineGradleArguments(String... args) {
        def arguments = ["--stacktrace"]
        arguments.addAll(Arrays.asList(args))
        return arguments
    }


    protected GradleRunner createGradleRunner() {
        return GradleRunner.create()
                .withProjectDir(testProjectRootDir)
                .withGradleVersion(gradleVersion.version)
                .withPluginClasspath()
                .forwardOutput()
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

    def getHadoopCatalog(File warehouse) {
        try {
            return new HadoopCatalog(new Configuration(), "file://" + warehouse.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
