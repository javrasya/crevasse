package com.crevasse.iceberg

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Types
import spock.lang.Specification
import spock.lang.TempDir

class MigrationExecutorSpec extends Specification {

    @TempDir
    File tempFolder


    @TempDir
    File icebergWarehouse


    def "should create table for the first migration step"() {
        given:
        def catalog = getHadoopCatalog()
        def tableIdentifier = TableIdentifier.of("testdb", "testtable")
        def migrationExecutor = getMigrationExecutor(
                catalog,
                tableIdentifier,
                [
                        migrate {
                            step 0
                            description """
                                * Add column 'id'
                                * Add column 'name'
                            """
                            addColumns {
                                stringCol('id', false)
                                stringCol('name', true)
                            }
                        }
                ]
        )

        when:
        migrationExecutor.run()

        then:
        def table = catalog.loadTable(tableIdentifier)

        table != null

        def columns = table.schema().columns().toSorted { it.fieldId() }
        columns.size() == 2

        columns.get(0).name() == "id"
        columns.get(0).type().typeId() == Type.TypeID.STRING
        columns.get(0).isRequired()

        columns.get(1).name() == "name"
        columns.get(1).type().typeId() == Type.TypeID.STRING
        !columns.get(1).isRequired()
    }

    def "should create all column types correctly"() {
        given:
        def catalog = getHadoopCatalog()
        def tableIdentifier = TableIdentifier.of("testdb", "testtable")
        def migrationExecutor = getMigrationExecutor(
                catalog,
                tableIdentifier,
                [
                        migrate {
                            step 0
                            description """
                                * Add all column types 
                            """
                            addColumns {
                                stringCol('stringCol', false)
                                intCol('integerCol', false)
                                longCol('longCol', false)
                                floatCol('floatCol', false)
                                doubleCol('doubleCol', false)
                                boolCol('booleanCol', false)
                                dateCol('dateCol', false)
                                timestampCol('timestampCol', false)
                                decimalCol('decimalCol', false, 10, 2)
                                fixedCol('fixedCol', false, 10)
                                binaryCol('binaryCol', false)
                                structCol('structCol', false) {
                                    stringCol('nestedStringCol', false)
                                }
                                listCol('listCol', false, stringType())
                                listCol('listColWithStructType', false, structType {
                                    stringCol('nestedStringCol', false)
                                })
                                mapCol('mapCol', false) {
                                    key(stringType())
                                    value(intType())
                                }
                                mapCol('mapColWithStructType', false) {
                                    key(stringType())
                                    value(structType {
                                        stringCol('nestedStringCol', false)
                                    })
                                }
                                timestampWithZoneCol('timestampWithZoneCol', false)
                            }
                        }
                ]
        )

        when:
        migrationExecutor.run()

        then:
        def table = catalog.loadTable(tableIdentifier)

        table != null

        def columns = table.schema().columns().toSorted { it.fieldId() }
        columns.size() == 17

        columns.get(0).name() == "stringCol"
        columns.get(0).type().typeId() == Type.TypeID.STRING
        columns.get(0).isRequired()

        columns.get(1).name() == "integerCol"
        columns.get(1).type().typeId() == Type.TypeID.INTEGER
        columns.get(1).isRequired()

        columns.get(2).name() == "longCol"
        columns.get(2).type().typeId() == Type.TypeID.LONG
        columns.get(2).isRequired()

        columns.get(3).name() == "floatCol"
        columns.get(3).type().typeId() == Type.TypeID.FLOAT
        columns.get(3).isRequired()

        columns.get(4).name() == "doubleCol"
        columns.get(4).type().typeId() == Type.TypeID.DOUBLE
        columns.get(4).isRequired()

        columns.get(5).name() == "booleanCol"
        columns.get(5).type().typeId() == Type.TypeID.BOOLEAN
        columns.get(5).isRequired()

        columns.get(6).name() == "dateCol"
        columns.get(6).type().typeId() == Type.TypeID.DATE
        columns.get(6).isRequired()

        columns.get(7).name() == "timestampCol"
        columns.get(7).type().typeId() == Type.TypeID.TIMESTAMP
        columns.get(7).isRequired()
        !((Types.TimestampType) columns.get(7).type()).shouldAdjustToUTC()

        columns.get(8).name() == "decimalCol"
        columns.get(8).type().typeId() == Type.TypeID.DECIMAL
        ((Types.DecimalType) columns.get(8).type()).precision() == 10
        ((Types.DecimalType) columns.get(8).type()).scale() == 2
        columns.get(8).isRequired()

        columns.get(9).name() == "fixedCol"
        columns.get(9).type().typeId() == Type.TypeID.FIXED
        ((Types.FixedType) columns.get(9).type()).length() == 10
        columns.get(9).isRequired()

        columns.get(10).name() == "binaryCol"
        columns.get(10).type().typeId() == Type.TypeID.BINARY
        columns.get(10).isRequired()

        columns.get(11).name() == "structCol"
        columns.get(11).type().typeId() == Type.TypeID.STRUCT
        columns.get(11).isRequired()
        columns.get(11).type().asStructType().fields().size() == 1
        columns.get(11).type().asStructType().fields().get(0).name() == "nestedStringCol"
        columns.get(11).type().asStructType().fields().get(0).type().typeId() == Type.TypeID.STRING
        columns.get(11).type().asStructType().fields().get(0).isRequired()

        columns.get(12).name() == "listCol"
        columns.get(12).type().typeId() == Type.TypeID.LIST
        columns.get(12).isRequired()
        columns.get(12).type().asListType().elementType().typeId() == Type.TypeID.STRING

        columns.get(13).name() == "listColWithStructType"
        columns.get(13).type().typeId() == Type.TypeID.LIST
        columns.get(13).isRequired()
        columns.get(13).type().asListType().elementType().typeId() == Type.TypeID.STRUCT
        columns.get(13).type().asListType().elementType().asStructType().fields().size() == 1
        columns.get(13).type().asListType().elementType().asStructType().fields().get(0).name() == "nestedStringCol"
        columns.get(13).type().asListType().elementType().asStructType().fields().get(0).type().typeId() == Type.TypeID.STRING
        columns.get(13).type().asListType().elementType().asStructType().fields().get(0).isRequired()

        columns.get(14).name() == "mapCol"
        columns.get(14).type().typeId() == Type.TypeID.MAP
        columns.get(14).isRequired()
        columns.get(14).type().asMapType().keyType().typeId() == Type.TypeID.STRING
        columns.get(14).type().asMapType().valueType().typeId() == Type.TypeID.INTEGER

        columns.get(15).name() == "mapColWithStructType"
        columns.get(15).type().typeId() == Type.TypeID.MAP
        columns.get(15).isRequired()
        columns.get(15).type().asMapType().keyType().typeId() == Type.TypeID.STRING
        columns.get(15).type().asMapType().valueType().typeId() == Type.TypeID.STRUCT
        columns.get(15).type().asMapType().valueType().asStructType().fields().size() == 1
        columns.get(15).type().asMapType().valueType().asStructType().fields().get(0).name() == "nestedStringCol"
        columns.get(15).type().asMapType().valueType().asStructType().fields().get(0).type().typeId() == Type.TypeID.STRING
        columns.get(15).type().asMapType().valueType().asStructType().fields().get(0).isRequired()

        columns.get(16).name() == "timestampWithZoneCol"
        columns.get(16).type().typeId() == Type.TypeID.TIMESTAMP
        columns.get(16).isRequired()
        ((Types.TimestampType) columns.get(16).type()).shouldAdjustToUTC()
    }

    def getMigrationExecutor(
            Catalog catalog,
            TableIdentifier tableIdentifier,
            List<MigrationBaseScript> migrationScripts) {
        return MigrationExecutor.builder()
                .tableIdentifier(tableIdentifier)
                .scriptDir(tempFolder.toPath())
                .catalogSupplier { catalog }
                .additionalMigrationScriptContainers(migrationScripts.collect { MigrationScriptContainer.fromScript(it) })
                .build()
    }


    def migrate(@DelegatesTo(MigrationStep) Closure closure) {
        def step = new MigrationStep()
        def script = new MigrationBaseScript()
        final Binding binding = new Binding();
        binding.setVariable("step", step);
        script.setBinding(binding)
        script.migrate(closure)
        return script
    }

    def getHadoopCatalog() {
        try {
            return new HadoopCatalog(new Configuration(), "file://" + icebergWarehouse.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
