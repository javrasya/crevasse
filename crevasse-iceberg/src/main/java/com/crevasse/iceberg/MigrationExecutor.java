package com.crevasse.iceberg;

import static com.crevasse.iceberg.helpers.MigrationHelpers.scanExistingMigrationScripts;

import com.crevasse.iceberg.helpers.MigrationHelpers;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Singular;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

public class MigrationExecutor {
  private static final String METADATA_STATE_KEY = "crevasse.migration.state";
  private static final String METADATA_LAST_UPDATED_AT_KEY = "crevasse.migration.last-applied-at";

  private final boolean dryRun;
  private final Supplier<Catalog> catalogSupplier;
  private final Path scriptDir;
  private final TableIdentifier tableIdentifier;
  private final List<MigrationScriptContainer> additionalMigrationScriptContainers;

  @Builder
  public MigrationExecutor(
      boolean dryRun,
      Path scriptDir,
      Supplier<Catalog> catalogSupplier,
      TableIdentifier tableIdentifier,
      @Singular List<MigrationScriptContainer> additionalMigrationScriptContainers) {
    this.dryRun = dryRun;
    this.scriptDir = scriptDir;
    this.catalogSupplier = catalogSupplier;
    this.tableIdentifier = tableIdentifier;
    this.additionalMigrationScriptContainers = additionalMigrationScriptContainers;
  }

  public MigrationExecutionResult run() throws IOException {
    final String database = tableIdentifier.namespace().toString();
    final String table = tableIdentifier.name();

    List<MigrationScriptContainer> migrationScriptContainers =
        new ArrayList<>(scanExistingMigrationScripts(scriptDir.toString(), tableIdentifier));
    migrationScriptContainers.addAll(additionalMigrationScriptContainers);

    if (migrationScriptContainers.isEmpty()) {
      return MigrationExecutionResult.noScripts(database, table);
    }

    final Catalog catalog = catalogSupplier.get();
    AtomicInteger latestAppliedMitigationId = new AtomicInteger(-1);
    if (catalog.tableExists(tableIdentifier)) {
      final Table icebergTable = catalog.loadTable(tableIdentifier);
      latestAppliedMitigationId.set(
          Integer.parseInt(icebergTable.properties().get(METADATA_STATE_KEY)));
    }

    final List<MigrationStep> migrationSteps = new ArrayList<>();
    for (MigrationScriptContainer migrationScriptContainer : migrationScriptContainers) {
      MigrationStep migrationStep = new MigrationStep();
      migrationScriptContainer.run(migrationStep);
      migrationSteps.add(migrationStep);
    }

    final List<MigrationStep> unappliedMitigationSteps =
        migrationSteps.stream()
            .filter(step -> step.getOrder() > latestAppliedMitigationId.get())
            .sorted(Comparator.comparing(MigrationStep::getOrder))
            .collect(Collectors.toList());

    if (unappliedMitigationSteps.isEmpty()) {
      return MigrationExecutionResult.noPending(database, table);
    }

    final int minOrder =
        unappliedMitigationSteps.stream()
            .map(MigrationStep::getOrder)
            .min(Comparator.naturalOrder())
            .orElse(-1);

    final int maxOrder =
        unappliedMitigationSteps.stream()
            .map(MigrationStep::getOrder)
            .max(Comparator.naturalOrder())
            .orElse(-1);

    final List<String> descriptions =
        unappliedMitigationSteps.stream()
            .map(step -> step.getDescription() != null ? step.getDescription().trim() : null)
            .collect(Collectors.toList());

    if (dryRun) {
      final MigrationContext migrationContextWithFakeTable =
          MigrationHelpers.getContextWithFakeTable();

      for (MigrationStep migrationStep : unappliedMitigationSteps) {
        migrationContextWithFakeTable.addStep(migrationStep);
      }

      migrationContextWithFakeTable.applyChanges();
      return MigrationExecutionResult.dryRun(
          database, table, unappliedMitigationSteps.size(), minOrder, maxOrder, descriptions);
    } else {
      final MigrationContext migrationContext = new MigrationContext(catalog, tableIdentifier);

      for (MigrationStep migrationStep : unappliedMitigationSteps) {
        migrationContext.addStep(migrationStep);
      }

      migrationContext.applyChanges();

      final Table icebergTable = catalog.loadTable(tableIdentifier);
      icebergTable
          .updateProperties()
          .set(METADATA_STATE_KEY, String.valueOf(maxOrder))
          .set(METADATA_LAST_UPDATED_AT_KEY, Instant.now().toString())
          .commit();

      return MigrationExecutionResult.applied(
          database, table, unappliedMitigationSteps.size(), minOrder, maxOrder, descriptions);
    }
  }
}
