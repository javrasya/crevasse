package com.crevasse.iceberg

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.types.Type
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

        def columns = table.schema().columns().toSorted { it.name() }
        columns.size() == 2

        columns.get(0).name() == "id"
        columns.get(0).type().typeId() == Type.TypeID.STRING
        columns.get(0).isRequired()

        columns.get(1).name() == "name"
        columns.get(1).type().typeId() == Type.TypeID.STRING
        !columns.get(1).isRequired()
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
