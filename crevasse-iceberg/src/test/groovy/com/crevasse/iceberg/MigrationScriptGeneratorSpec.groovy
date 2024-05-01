package com.crevasse.iceberg

import com.crevasse.iceberg.helpers.AvroSchemaBuilder
import com.crevasse.iceberg.schema.ListColumnType
import com.crevasse.iceberg.schema.MapColumnType
import com.crevasse.iceberg.schema.StructColumnType
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

        def migrationScriptGenerator = MigrationScriptGenerator.builder()
                .scriptPath(tempFolder.toPath())
                .avroSchema(schema)
                .build()

        when:
        migrationScriptGenerator.generateMigration()

        then:

        def migrationScriptContainers = tempFolder.listFiles()
        migrationScriptContainers.size() == 1

        def generatedMigrationScriptFiles = migrationScriptContainers[0].listFiles()
        generatedMigrationScriptFiles.size() == 1

        def firstMigrationScript = generatedMigrationScriptFiles[0]
        firstMigrationScript.name == "migration_0.groovy"

        def migrationScript = MigrationScript.fromPath(firstMigrationScript.toPath())
        def migrationStep = new MigrationStep()
        migrationScript.run(migrationStep)

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


    static def avroSchema(String schemaName, @DelegatesTo(AvroSchemaBuilder) Closure closure) {
        def builder = new AvroSchemaBuilder(schemaName)
        closure.delegate = builder
        closure.resolveStrategy = Closure.DELEGATE_ONLY
        closure.call()
        builder.build()
    }
}
