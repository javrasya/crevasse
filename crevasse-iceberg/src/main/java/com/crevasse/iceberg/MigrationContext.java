package com.crevasse.iceberg;

import com.crevasse.relocated.com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

public class MigrationContext {

  private final TableIdentifier tableIdentifier;
  private final Catalog catalog;
  private Table table;

  private final List<MigrationStep> steps = new ArrayList<>();

  public MigrationContext(Catalog catalog, TableIdentifier tableIdentifier) {
    this.catalog = catalog;
    this.tableIdentifier = tableIdentifier;
  }

  void initialize() {
    if (catalog.tableExists(tableIdentifier)) {
      this.table = catalog.loadTable(tableIdentifier);
    }
  }

  void refreshTable() {
    Preconditions.checkArgument(this.table != null, "Table is not initialized");
    this.table.refresh();
  }

  public void addStep(MigrationStep step) {
    this.steps.add(step);
  }

  public boolean hasAnyStep() {
    return !this.steps.isEmpty();
  }

  public Schema getIcebergSchema() {
    return this.table.schema();
  }

  public void applyChanges() {
    if (this.steps.isEmpty()) {
      return;
    }
    final List<MigrationStep> sortedSteps =
        this.steps.stream()
            .sorted(Comparator.comparing(MigrationStep::getOrder))
            .collect(Collectors.toList());

    initialize();
    int offset = 0;
    if (this.table == null) {
      final MigrationStep firstStep = sortedSteps.get(0);
      this.table = firstStep.applyCreateTable(catalog, tableIdentifier);
      offset = 1;
    }

    final List<MigrationStep> followingSteps =
        sortedSteps.stream().skip(offset).collect(Collectors.toList());
    for (MigrationStep step : followingSteps) {
      step.applyOn(this.table);
      this.refreshTable();
    }
  }
}
