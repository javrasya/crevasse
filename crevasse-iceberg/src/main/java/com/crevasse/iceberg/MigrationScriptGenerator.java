package com.crevasse.iceberg;

import static com.crevasse.iceberg.helpers.MigrationHelpers.getContextWithFakeTable;
import static com.crevasse.iceberg.helpers.MigrationHelpers.scanExistingMigrationScripts;
import static com.crevasse.iceberg.helpers.SchemaHelper.calculateSchemaDifferences;

import com.crevasse.iceberg.ops.TableOperation;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import lombok.Builder;
import lombok.Singular;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

public class MigrationScriptGenerator implements Serializable {

  private final Path scriptDir;
  private final TableIdentifier tableIdentifier;
  private final org.apache.avro.Schema avroSchema;
  private final List<String> ignoredColumns;

  @Builder
  public MigrationScriptGenerator(
      Path scriptDir,
      TableIdentifier tableIdentifier,
      org.apache.avro.Schema avroSchema,
      @Singular List<String> ignoredColumns) {
    this.scriptDir = scriptDir;
    this.tableIdentifier = tableIdentifier;
    this.avroSchema = avroSchema;
    this.ignoredColumns = ignoredColumns;
  }

  public MigrationGenerationResult generateMigration() throws IOException {
    final MigrationContext contextWithFakeTable = getContextWithFakeTable();
    final List<MigrationScriptContainer> groovyScriptFiles =
        scanExistingMigrationScripts(scriptDir.toString(), tableIdentifier);

    final List<MigrationStep> migrationSteps = new ArrayList<>();
    for (MigrationScriptContainer groovyScriptFile : groovyScriptFiles) {
      MigrationStep migrationStep = new MigrationStep();
      groovyScriptFile.run(migrationStep);
      migrationSteps.add(migrationStep);
    }

    final int maxOrderSoFar =
        migrationSteps.stream()
            .map(MigrationStep::getOrder)
            .max(Comparator.naturalOrder())
            .orElse(-1);

    for (MigrationStep migrationStep : migrationSteps) {
      contextWithFakeTable.addStep(migrationStep);
    }

    contextWithFakeTable.applyChanges();

    final List<TableOperation> tableOperations = getNextTableOperations(contextWithFakeTable);
    final String database = tableIdentifier.namespace().toString();
    final String table = tableIdentifier.name();

    if (tableOperations.isEmpty()) {
      return MigrationGenerationResult.noChanges(database, table);
    }

    final int newStepNumber = maxOrderSoFar + 1;
    Path generatedScriptPath =
        MigrationScriptContainer.generateAndGetPath(
            scriptDir.toString(),
            newStepNumber,
            tableOperations,
            database,
            table);

    List<String> operationDescriptions =
        tableOperations.stream().flatMap(op -> op.getDescriptions().stream()).toList();

    return MigrationGenerationResult.created(
        database, table, generatedScriptPath, newStepNumber, operationDescriptions);
  }

  private List<TableOperation> getNextTableOperations(MigrationContext contextWithFakeTable) {
    final Types.StructType newSchema = getIcebergSchema().asStruct();
    final Types.StructType oldSchema =
        contextWithFakeTable.hasAnyStep()
            ? contextWithFakeTable.getIcebergSchema().asStruct()
            : null;
    final SchemaDifferences schemaDifferences =
        calculateSchemaDifferences(oldSchema, newSchema, ignoredColumns);
    return schemaDifferences.getTableOperations();
  }

  private Schema getIcebergSchema() {
    return AvroSchemaUtil.toIceberg(avroSchema);
  }
}
