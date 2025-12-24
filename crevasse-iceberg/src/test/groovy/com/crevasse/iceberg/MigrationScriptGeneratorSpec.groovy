package com.crevasse.iceberg

import com.crevasse.iceberg.helpers.AvroSchemaBuilder
import com.crevasse.iceberg.schema.ListColumnType
import com.crevasse.iceberg.schema.MapColumnType
import com.crevasse.iceberg.schema.StructColumnType
import org.apache.avro.Schema
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.types.Type
import spock.lang.Specification
import spock.lang.TempDir

import static org.apache.avro.Schema.Type.INT
import static org.apache.avro.Schema.Type.STRING

class MigrationScriptGeneratorSpec extends Specification {

    @TempDir
    File tempFolder

    def "should generate a migration file"() {
        given:
        def schema = avroSchema("TestSchema") {
            requiredInt("id")
            requiredStruct("nested1", "NestedSchema") {
                requiredInt("nested1Id")
                requiredString("nested2Id")
            }
            requiredMapOfPrimitive("mapField1", STRING)
            requiredArrayOfPrimitive("arrayField1", INT)
        }

        def migrationScriptGenerator = getMigrationScriptGenerator(schema)

        when:
        def result = migrationScriptGenerator.generateMigration()

        then:
        result.status == MigrationGenerationResult.Status.CREATED
        result.database == "test"
        result.table == "test"
        result.stepNumber == 0
        result.scriptPath != null
        result.operations.size() == 4

        def namespaceFolders = tempFolder.listFiles()
        namespaceFolders.size() == 1

        def tableFolders = namespaceFolders[0].listFiles()
        tableFolders.size() == 1

        def generatedMigrationScriptFiles = tableFolders[0].listFiles()
        generatedMigrationScriptFiles.size() == 1

        def firstMigrationScript = generatedMigrationScriptFiles[0]
        firstMigrationScript.name == "migration_0.groovy"

        def migrationScriptContainer = MigrationScriptContainer.fromPath(firstMigrationScript.toPath())
        def migrationStep = new MigrationStep()
        migrationScriptContainer.run(migrationStep)

        migrationStep.addedColumns.size() == 4
        migrationStep.addedColumns.containsKey("id")
        migrationStep.addedColumns.containsKey("nested1")
        migrationStep.addedColumns.containsKey("mapField1")
        migrationStep.addedColumns.containsKey("arrayField1")

        migrationStep.addedColumns.get("id").columnType.typeId == Type.TypeID.INTEGER

        migrationStep.addedColumns.get("nested1").columnType.typeId == Type.TypeID.STRUCT
        (migrationStep.addedColumns.get("nested1").columnType as StructColumnType).columns.size() == 2
        (migrationStep.addedColumns.get("nested1").columnType as StructColumnType).columns*.name == ["nested1Id", "nested2Id"]
        (migrationStep.addedColumns.get("nested1").columnType as StructColumnType).columns*.columnType.typeId == [Type.TypeID.INTEGER, Type.TypeID.STRING]


        migrationStep.addedColumns.get("mapField1").columnType.typeId == Type.TypeID.MAP
        (migrationStep.addedColumns.get("mapField1").columnType as MapColumnType).keyType.typeId == Type.TypeID.STRING
        (migrationStep.addedColumns.get("mapField1").columnType as MapColumnType).valueType.typeId == Type.TypeID.STRING

        migrationStep.addedColumns.get("arrayField1").columnType.typeId == Type.TypeID.LIST
        (migrationStep.addedColumns.get("arrayField1").columnType as ListColumnType).elementType.typeId == Type.TypeID.INTEGER

        migrationStep.removedColumns.size() == 0
        migrationStep.columnsMarkedAsRequired.size() == 0
        migrationStep.columnsMarkedAsOptional.size() == 0
        migrationStep.updatedColumnTypes.size() == 0
    }

    def "should remove column when the schema no longer has it"() {
        given:
        def schema = avroSchema("TestSchema") {
            requiredInt("id")
            requiredStruct("nested1", "NestedSchema") {
                requiredInt("nested1Id")
                requiredString("nested2Id")
            }
            requiredMapOfPrimitive("mapField1", STRING)
            requiredArrayOfPrimitive("arrayField1", INT)
        }

        def newSchemaVersion = avroSchema("TestSchema") {
            requiredStruct("nested1", "NestedSchema") {
                requiredInt("nested1Id")
                requiredString("nested2Id")
            }
            requiredMapOfPrimitive("mapField1", STRING)
            requiredArrayOfPrimitive("arrayField1", INT)
        }

        def migrationScriptGenerator = getMigrationScriptGenerator(schema)

        def migrationScriptGenerator2 = getMigrationScriptGenerator(newSchemaVersion)

        when:
        def result1 = migrationScriptGenerator.generateMigration()

        then:
        result1.status == MigrationGenerationResult.Status.CREATED
        result1.stepNumber == 0

        def namespaceFolders = tempFolder.listFiles()
        namespaceFolders.size() == 1

        def tableFolders = namespaceFolders[0].listFiles()
        tableFolders.size() == 1

        def generatedMigrationScriptFiles = tableFolders[0].listFiles()
        generatedMigrationScriptFiles.size() == 1

        def firstMigrationScript = generatedMigrationScriptFiles[0]
        firstMigrationScript.name == "migration_0.groovy"

        when:
        def result2 = migrationScriptGenerator2.generateMigration()

        then:
        result2.status == MigrationGenerationResult.Status.CREATED
        result2.stepNumber == 1
        result2.operations.size() == 1

        def namespaceFolders2 = tempFolder.listFiles()
        namespaceFolders2.size() == 1

        def tableFolders2 = namespaceFolders2[0].listFiles()
        tableFolders2.size() == 1

        def generatedMigrationScriptFiles2 = tableFolders2[0].listFiles().sort { it.name }
        generatedMigrationScriptFiles2.size() == 2

        def secondMigrationScript = generatedMigrationScriptFiles2[1]
        secondMigrationScript.name == "migration_1.groovy"


        def migrationStep = new MigrationStep()
        def migrationScriptContainer = MigrationScriptContainer.fromPath(secondMigrationScript.toPath())
        migrationScriptContainer.run(migrationStep)

        migrationStep.addedColumns.size() == 0
        migrationStep.removedColumns.size() == 1
        migrationStep.removedColumns.contains("id")
        migrationStep.columnsMarkedAsRequired.size() == 0
        migrationStep.columnsMarkedAsOptional.size() == 0
        migrationStep.updatedColumnTypes.size() == 0
    }

    def "should  add new column"() {
        given:
        def schema = avroSchema("TestSchema") {
            requiredInt("id")
            requiredStruct("nested1", "NestedSchema") {
                requiredInt("nested1Id")
                requiredString("nested2Id")
            }
            requiredMapOfPrimitive("mapField1", STRING)
            requiredArrayOfPrimitive("arrayField1", INT)
        }

        def newSchemaVersion = avroSchema("TestSchema") {
            requiredInt("id")
            requiredStruct("nested1", "NestedSchema") {
                requiredInt("nested1Id")
                requiredString("nested2Id")
            }
            requiredMapOfPrimitive("mapField1", STRING)
            requiredArrayOfPrimitive("arrayField1", INT)
            requiredString("newField")
        }

        def migrationScriptGenerator = getMigrationScriptGenerator(schema)

        def migrationScriptGenerator2 = getMigrationScriptGenerator(newSchemaVersion)

        when:
        def result1 = migrationScriptGenerator.generateMigration()

        then:
        result1.status == MigrationGenerationResult.Status.CREATED
        result1.stepNumber == 0

        def namespaceFolders = tempFolder.listFiles()
        namespaceFolders.size() == 1

        def tableFolders = namespaceFolders[0].listFiles()
        tableFolders.size() == 1

        def generatedMigrationScriptFiles = tableFolders[0].listFiles()
        generatedMigrationScriptFiles.size() == 1

        def firstMigrationScript = generatedMigrationScriptFiles[0]
        firstMigrationScript.name == "migration_0.groovy"

        when:
        def result2 = migrationScriptGenerator2.generateMigration()

        then:
        result2.status == MigrationGenerationResult.Status.CREATED
        result2.stepNumber == 1
        result2.operations.size() == 1

        def namespaceFolders2 = tempFolder.listFiles()
        namespaceFolders2.size() == 1

        def tableFolders2 = namespaceFolders2[0].listFiles()
        tableFolders2.size() == 1

        def generatedMigrationScriptFiles2 = tableFolders2[0].listFiles().sort { it.name }
        generatedMigrationScriptFiles2.size() == 2

        def secondMigrationScript = generatedMigrationScriptFiles2[1]
        secondMigrationScript.name == "migration_1.groovy"

        def migrationStep = new MigrationStep()
        def migrationScriptContainer = MigrationScriptContainer.fromPath(secondMigrationScript.toPath())
        migrationScriptContainer.run(migrationStep)

        migrationStep.addedColumns.size() == 1
        migrationStep.addedColumns.containsKey("newField")
        migrationStep.addedColumns.get("newField").columnType.typeId == Type.TypeID.STRING

        migrationStep.removedColumns.size() == 0
        migrationStep.columnsMarkedAsRequired.size() == 0
        migrationStep.columnsMarkedAsOptional.size() == 0
        migrationStep.updatedColumnTypes.size() == 0
    }

    def "should mark column as required"() {
        given:
        def schema = avroSchema("TestSchema") {
            optionalString("id")
            requiredStruct("nested1", "NestedSchema") {
                requiredInt("nested1Id")
                requiredString("nested2Id")
            }
            requiredMapOfPrimitive("mapField1", STRING)
            requiredArrayOfPrimitive("arrayField1", INT)
        }

        def newSchemaVersion = avroSchema("TestSchema") {
            requiredInt("id")
            requiredStruct("nested1", "NestedSchema") {
                requiredInt("nested1Id")
                requiredString("nested2Id")
            }
            requiredMapOfPrimitive("mapField1", STRING)
            requiredArrayOfPrimitive("arrayField1", INT)
        }

        def migrationScriptGenerator = getMigrationScriptGenerator(schema)

        def migrationScriptGenerator2 = getMigrationScriptGenerator(newSchemaVersion)

        when:
        def result1 = migrationScriptGenerator.generateMigration()

        then:
        result1.status == MigrationGenerationResult.Status.CREATED
        result1.stepNumber == 0

        def namespaceFolders = tempFolder.listFiles()
        namespaceFolders.size() == 1

        def tableFolders = namespaceFolders[0].listFiles()
        tableFolders.size() == 1

        def generatedMigrationScriptFiles = tableFolders[0].listFiles()
        generatedMigrationScriptFiles.size() == 1

        def firstMigrationScript = generatedMigrationScriptFiles[0]
        firstMigrationScript.name == "migration_0.groovy"

        when:
        def result2 = migrationScriptGenerator2.generateMigration()

        then:
        result2.status == MigrationGenerationResult.Status.CREATED
        result2.stepNumber == 1
        result2.operations.size() == 1

        def namespaceFolders2 = tempFolder.listFiles()
        namespaceFolders2.size() == 1

        def tableFolders2 = namespaceFolders2[0].listFiles()
        tableFolders2.size() == 1

        def generatedMigrationScriptFiles2 = tableFolders2[0].listFiles().sort { it.name }
        generatedMigrationScriptFiles2.size() == 2

        def secondMigrationScript = generatedMigrationScriptFiles2[1]
        secondMigrationScript.name == "migration_1.groovy"

        def migrationStep = new MigrationStep()
        def migrationScriptContainer = MigrationScriptContainer.fromPath(secondMigrationScript.toPath())
        migrationScriptContainer.run(migrationStep)

        migrationStep.addedColumns.size() == 0
        migrationStep.removedColumns.size() == 0
        migrationStep.columnsMarkedAsRequired.size() == 1
        migrationStep.columnsMarkedAsRequired.contains("id")
        migrationStep.columnsMarkedAsOptional.size() == 0
        migrationStep.updatedColumnTypes.size() == 0
    }

    def "should mark column as optional"() {
        given:
        def schema = avroSchema("TestSchema") {
            requiredString("id")
            requiredStruct("nested1", "NestedSchema") {
                requiredInt("nested1Id")
                requiredString("nested2Id")
            }
            requiredMapOfPrimitive("mapField1", STRING)
            requiredArrayOfPrimitive("arrayField1", INT)
        }

        def newSchemaVersion = avroSchema("TestSchema") {
            optionalString("id")
            requiredStruct("nested1", "NestedSchema") {
                requiredInt("nested1Id")
                requiredString("nested2Id")
            }
            requiredMapOfPrimitive("mapField1", STRING)
            requiredArrayOfPrimitive("arrayField1", INT)
        }

        def migrationScriptGenerator = getMigrationScriptGenerator(schema)

        def migrationScriptGenerator2 = getMigrationScriptGenerator(newSchemaVersion)

        when:
        def result1 = migrationScriptGenerator.generateMigration()

        then:
        result1.status == MigrationGenerationResult.Status.CREATED
        result1.stepNumber == 0

        def namespaceFolders = tempFolder.listFiles()
        namespaceFolders.size() == 1

        def tableFolders = namespaceFolders[0].listFiles()
        tableFolders.size() == 1

        def generatedMigrationScriptFiles = tableFolders[0].listFiles()
        generatedMigrationScriptFiles.size() == 1

        def firstMigrationScript = generatedMigrationScriptFiles[0]
        firstMigrationScript.name == "migration_0.groovy"

        when:
        def result2 = migrationScriptGenerator2.generateMigration()

        then:
        result2.status == MigrationGenerationResult.Status.CREATED
        result2.stepNumber == 1
        result2.operations.size() == 1

        def namespaceFolders2 = tempFolder.listFiles()
        namespaceFolders2.size() == 1

        def tableFolders2 = namespaceFolders2[0].listFiles()
        tableFolders2.size() == 1

        def generatedMigrationScriptFiles2 = tableFolders2[0].listFiles().sort { it.name }
        generatedMigrationScriptFiles2.size() == 2

        def secondMigrationScript = generatedMigrationScriptFiles2[1]
        secondMigrationScript.name == "migration_1.groovy"

        def migrationStep = new MigrationStep()
        def migrationScriptContainer = MigrationScriptContainer.fromPath(secondMigrationScript.toPath())
        migrationScriptContainer.run(migrationStep)

        migrationStep.addedColumns.size() == 0
        migrationStep.removedColumns.size() == 0
        migrationStep.columnsMarkedAsRequired.size() == 0
        migrationStep.columnsMarkedAsOptional.size() == 1
        migrationStep.columnsMarkedAsOptional.contains("id")
        migrationStep.updatedColumnTypes.size() == 0
    }

    def "should update primitive column type"() {
        given:
        def schema = avroSchema("TestSchema") {
            requiredInt("id")
            requiredStruct("nested1", "NestedSchema") {
                requiredInt("nested1Id")
                requiredString("nested2Id")
            }
            requiredMapOfPrimitive("mapField1", STRING)
            requiredArrayOfPrimitive("arrayField1", INT)
        }

        def newSchemaVersion = avroSchema("TestSchema") {
            requiredLong("id")
            requiredStruct("nested1", "NestedSchema") {
                requiredInt("nested1Id")
                requiredString("nested2Id")
            }
            requiredMapOfPrimitive("mapField1", STRING)
            requiredArrayOfPrimitive("arrayField1", INT)
        }

        def migrationScriptGenerator = getMigrationScriptGenerator(schema)

        def migrationScriptGenerator2 = getMigrationScriptGenerator(newSchemaVersion)

        when:
        def result1 = migrationScriptGenerator.generateMigration()

        then:
        result1.status == MigrationGenerationResult.Status.CREATED
        result1.stepNumber == 0

        def namespaceFolders = tempFolder.listFiles()
        namespaceFolders.size() == 1

        def tableFolders = namespaceFolders[0].listFiles()
        tableFolders.size() == 1

        def generatedMigrationScriptFiles = tableFolders[0].listFiles()
        generatedMigrationScriptFiles.size() == 1

        def firstMigrationScript = generatedMigrationScriptFiles[0]
        firstMigrationScript.name == "migration_0.groovy"

        when:
        def result2 = migrationScriptGenerator2.generateMigration()

        then:
        result2.status == MigrationGenerationResult.Status.CREATED
        result2.stepNumber == 1
        result2.operations.size() == 1

        def namespaceFolders2 = tempFolder.listFiles()
        namespaceFolders2.size() == 1

        def tableFolders2 = namespaceFolders2[0].listFiles()
        tableFolders2.size() == 1

        def generatedMigrationScriptFiles2 = tableFolders2[0].listFiles().sort { it.name }
        generatedMigrationScriptFiles2.size() == 2

        def secondMigrationScript = generatedMigrationScriptFiles2[1]
        secondMigrationScript.name == "migration_1.groovy"

        def migrationStep = new MigrationStep()
        def migrationScriptContainer = MigrationScriptContainer.fromPath(secondMigrationScript.toPath())
        migrationScriptContainer.run(migrationStep)

        migrationStep.addedColumns.size() == 0
        migrationStep.removedColumns.size() == 0
        migrationStep.columnsMarkedAsRequired.size() == 0
        migrationStep.columnsMarkedAsOptional.size() == 0
        migrationStep.updatedColumnTypes.size() == 1
        migrationStep.updatedColumnTypes.containsKey("id")
        migrationStep.updatedColumnTypes.get("id").typeId == Type.TypeID.LONG
    }

    def "should return SKIPPED_NO_CHANGES when schema is unchanged"() {
        given:
        def schema = avroSchema("TestSchema") {
            requiredInt("id")
            requiredString("name")
        }

        def migrationScriptGenerator = getMigrationScriptGenerator(schema)

        when:
        def result1 = migrationScriptGenerator.generateMigration()

        then:
        result1.status == MigrationGenerationResult.Status.CREATED
        result1.stepNumber == 0

        when:
        def result2 = migrationScriptGenerator.generateMigration()

        then:
        result2.status == MigrationGenerationResult.Status.SKIPPED_NO_CHANGES
        result2.database == "test"
        result2.table == "test"
        result2.scriptPath == null
        result2.operations == null
    }


    private MigrationScriptGenerator getMigrationScriptGenerator(Schema schema) {
        MigrationScriptGenerator.builder()
                .tableIdentifier(TableIdentifier.of("test", "test"))
                .scriptDir(tempFolder.toPath())
                .avroSchema(schema)
                .build()
    }

    static def avroSchema(String schemaName, @DelegatesTo(AvroSchemaBuilder) Closure closure) {
        def builder = new AvroSchemaBuilder(schemaName)
        closure.delegate = builder
        closure.resolveStrategy = Closure.DELEGATE_ONLY
        closure.call()
        builder.build()
    }
}
