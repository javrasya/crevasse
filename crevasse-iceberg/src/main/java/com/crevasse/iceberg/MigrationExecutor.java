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

  public void run() throws IOException {
    List<MigrationScriptContainer> migrationScriptContainers =
        new ArrayList<>(scanExistingMigrationScripts(scriptDir.toString(), tableIdentifier));
    migrationScriptContainers.addAll(additionalMigrationScriptContainers);

    if (migrationScriptContainers.isEmpty()) {
      System.out.println("No migration scripts found to apply");
      return;
    }

    final Catalog catalog = catalogSupplier.get();
    AtomicInteger latestAppliedMitigationId = new AtomicInteger(-1);
    if (catalog.tableExists(tableIdentifier)) {
      final Table table = catalog.loadTable(tableIdentifier);
      latestAppliedMitigationId.set(Integer.parseInt(table.properties().get(METADATA_STATE_KEY)));
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
            .collect(Collectors.toList());

    final int maxOrderSoFar =
        unappliedMitigationSteps.stream()
            .map(MigrationStep::getOrder)
            .max(Comparator.naturalOrder())
            .orElse(-1);

    if (dryRun) {
      System.out.println("Dry run mode enabled. No migration will be applied");

      final MigrationContext migrationContextWithFakeTable =
          MigrationHelpers.getContextWithFakeTable();

      for (MigrationStep migrationStep : unappliedMitigationSteps) {
        System.out.printf(
            "Applying migration in dryRun mode with description of '%s'%n",
            migrationStep.getDescription());
        migrationContextWithFakeTable.addStep(migrationStep);
      }

      migrationContextWithFakeTable.applyChanges();
    } else {
      final MigrationContext migrationContext = new MigrationContext(catalog, tableIdentifier);

      for (MigrationStep migrationStep : unappliedMitigationSteps) {
        System.out.printf(
            "Registering migration with description of '%s'%n", migrationStep.getDescription());
        migrationContext.addStep(migrationStep);
      }

      if (migrationContext.hasAnyStep()) {
        migrationContext.applyChanges();

        final Table table = catalog.loadTable(tableIdentifier);
        table
            .updateProperties()
            .set(METADATA_STATE_KEY, String.valueOf(maxOrderSoFar))
            .set(METADATA_LAST_UPDATED_AT_KEY, Instant.now().toString())
            .commit();
        System.out.printf(
            "Migration with %d steps has been applied%n", unappliedMitigationSteps.size());
      } else {
        System.out.println("No migration found to apply");
      }
    }
  }
}
