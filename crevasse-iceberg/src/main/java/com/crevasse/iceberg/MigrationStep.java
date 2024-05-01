package com.crevasse.iceberg;

import static com.crevasse.iceberg.PartitionColumnHandler.availablePartitionColumnSuffixes;
import static com.crevasse.iceberg.helpers.MigrationHelpers.getFakeIcebergCatalog;

import com.crevasse.iceberg.PartitionColumnHandler.PartitionColumn;
import com.crevasse.iceberg.schema.Column;
import com.crevasse.iceberg.schema.PrimitiveColumnType;
import com.crevasse.relocated.com.google.common.base.Preconditions;
import com.crevasse.relocated.com.google.common.collect.Sets;
import groovy.lang.Closure;
import groovy.lang.DelegatesTo;
import groovy.transform.PackageScope;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

public class MigrationStep {

  private int _order;
  private String _description;

  @PackageScope final Map<String, Column> addedColumns = new HashMap<>();
  @PackageScope final Set<String> removedColumns = new HashSet<>();
  @PackageScope final Set<String> columnsMarkedAsRequired = new HashSet<>();
  @PackageScope final Set<String> columnsMarkedAsOptional = new HashSet<>();
  @PackageScope final Map<String, PrimitiveColumnType> updatedColumnTypes = new HashMap<>();

  private final Map<String, List<Column>> addedColumnsToParent = new HashMap<>();

  private final List<PartitionColumn> addedPartitionColumns = new ArrayList<>();
  private final List<String> removedPartitionColumns = new ArrayList<>();

  private final Map<String, String> addedTableProperties = new HashMap<>();
  private final Set<String> removedTableProperties = new HashSet<>();

  private final AtomicBoolean anyIncompatibleChange = new AtomicBoolean(false);

  public void step(int step) {
    Preconditions.checkArgument(step >= 0, "Step must be a non-negative integer");
    _order = step;
  }

  public void description(String description) {
    Preconditions.checkArgument(description != null, "Description must be a non-null string");
    Preconditions.checkArgument(
        !description.replaceAll("\n", "").trim().isEmpty(),
        "Description must be a non-empty string");
    _description = description;
  }

  public void addColumns(@DelegatesTo(StructTypeHandler.class) Closure closure) {
    StructTypeHandler structTypeHandler = new StructTypeHandler();
    closure.setDelegate(structTypeHandler);
    closure.setResolveStrategy(Closure.DELEGATE_ONLY);
    closure.call();
    structTypeHandler
        .getColumns()
        .forEach(
            column -> {
              addedColumns.put(column.getName(), column);
              removedColumns.remove(column.getName());

              if (column.isRequired()) {
                anyIncompatibleChange.set(true);
              }
            });
  }

  public void addColumnsToParent(
      String parent, @DelegatesTo(StructTypeHandler.class) Closure closure) {
    StructTypeHandler structTypeHandler = new StructTypeHandler();
    closure.setDelegate(structTypeHandler);
    closure.setResolveStrategy(Closure.DELEGATE_ONLY);
    closure.call();
    structTypeHandler
        .getColumns()
        .forEach(
            column -> {
              if (removedColumns.contains(parent)) {
                throw new IllegalStateException(
                    "Cannot add a column to a parent that is being removed");
              }

              List<Column> columns = addedColumnsToParent.getOrDefault(parent, new ArrayList<>());
              columns.add(column);
              addedColumnsToParent.put(parent, columns);
              anyIncompatibleChange.set(true);
            });
  }

  public void removeColumn(String columnName) {
    if (addedColumns.containsKey(columnName)) {
      addedColumns.remove(columnName);
    } else {
      removedColumns.add(columnName);
      anyIncompatibleChange.set(true);
    }
  }

  public void modifyColumns(@DelegatesTo(ModifyColumnsHandler.class) Closure closure) {
    ModifyColumnsHandler modifyColumnsHandler = new ModifyColumnsHandler();
    closure.setDelegate(modifyColumnsHandler);
    closure.setResolveStrategy(Closure.DELEGATE_ONLY);
    closure.call();

    final Set<String> allUpdatedColumns = modifyColumnsHandler.getAllUpdatedColumns();
    Sets.intersection(allUpdatedColumns, removedColumns).stream()
        .findFirst()
        .ifPresent(
            removedColumn -> {
              throw new IllegalStateException("Cannot modify a column that is being removed");
            });

    Sets.intersection(allUpdatedColumns, addedColumns.keySet()).stream()
        .findFirst()
        .ifPresent(
            removedColumn -> {
              throw new IllegalStateException("Cannot modify a column that is being added");
            });

    columnsMarkedAsRequired.addAll(modifyColumnsHandler.getColumnsMarkedAsRequired());
    columnsMarkedAsOptional.addAll(modifyColumnsHandler.getColumnsMarkedAsOptional());
    updatedColumnTypes.putAll(modifyColumnsHandler.getUpdatedColumnTypes());

    if (!modifyColumnsHandler.getColumnsMarkedAsRequired().isEmpty()
        || !modifyColumnsHandler.getUpdatedColumnTypes().isEmpty()) {
      anyIncompatibleChange.set(true);
    }
  }

  public void addPartitionColumns(@DelegatesTo(PartitionColumnHandler.class) Closure closure) {
    PartitionColumnHandler partitionColumnHandler = new PartitionColumnHandler();
    closure.setDelegate(partitionColumnHandler);
    closure.setResolveStrategy(Closure.DELEGATE_ONLY);
    closure.call();
    addedPartitionColumns.addAll(partitionColumnHandler.getPartitionColumns());
  }

  public void removePartitionColumn(String columnName) {
    removedPartitionColumns.add(columnName);
  }

  public void addProperty(String key, String value) {
    removedTableProperties.remove(key);
    addedTableProperties.put(key, value);
  }

  public void removeProperty(String key) {
    addedTableProperties.remove(key);
    removedTableProperties.add(key);
  }

  public void addProperties(Map<String, String> properties) {
    addedTableProperties.putAll(properties);
    removedTableProperties.removeAll(properties.keySet());
  }

  Table applyCreateTable(Catalog catalog, TableIdentifier tableIdentifier) {
    Preconditions.checkArgument(
        this._order == 0, "Only the first migration step can create a table");

    final Schema schema = normalizedSchemaAndGet();

    final Transaction transaction = catalog.newCreateTableTransaction(tableIdentifier, schema);
    updatePartitionSpec(transaction);
    updateTableProperties(transaction);

    transaction.commitTransaction();
    return catalog.loadTable(tableIdentifier);
  }

  void applyOn(Table table) {
    final Transaction transaction = table.newTransaction();

    updateSchema(transaction);
    updatePartitionSpec(transaction);
    updateTableProperties(transaction);

    transaction.commitTransaction();
  }

  private Schema normalizedSchemaAndGet() {
    Catalog fakeIcebergCatalog = getFakeIcebergCatalog();
    Table fakeTable =
        fakeIcebergCatalog.createTable(
            TableIdentifier.of("fake_table"),
            new Schema(
                Collections.singletonList(
                    Types.NestedField.of(0, true, "__dummy__", Types.StringType.get()))));
    final UpdateSchema updateSchema = fakeTable.updateSchema().allowIncompatibleChanges();
    addColumns(updateSchema);
    updateSchema.deleteColumn("__dummy__");
    updateSchema.commit();
    fakeTable.refresh();

    final AtomicInteger lastColumnId = new AtomicInteger(0);

    return TypeUtil.assignFreshIds(fakeTable.schema(), lastColumnId::incrementAndGet);
  }

  private void updateSchema(Transaction transaction) {
    final UpdateSchema updateSchema = transaction.updateSchema();
    if (anyIncompatibleChange.get()) {
      updateSchema.allowIncompatibleChanges();
    }

    addColumns(updateSchema);
    addColumnsToParent(updateSchema);
    modifyColumns(updateSchema);
    removeColumns(updateSchema);
    updateSchema.commit();
  }

  private void updatePartitionSpec(Transaction transaction) {
    UpdatePartitionSpec updatePartitionSpec = transaction.updateSpec();
    removePartitionColumns(updatePartitionSpec, transaction.table().spec());
    addPartitionColumns(updatePartitionSpec);
    updatePartitionSpec.commit();
  }

  private void updateTableProperties(Transaction transaction) {
    UpdateProperties updateProperties = transaction.updateProperties();
    addProperties(updateProperties);
    removeProperties(updateProperties);
    updateProperties.commit();
  }

  void addColumns(UpdateSchema updateSchema) {
    for (Column addedColumn : addedColumns.values()) {
      if (addedColumn.isRequired()) {
        updateSchema.addRequiredColumn(
            addedColumn.getName(), addedColumn.getType(), addedColumn.getDoc());
      } else {
        updateSchema.addColumn(addedColumn.getName(), addedColumn.getType(), addedColumn.getDoc());
      }
    }
  }

  void addColumnsToParent(UpdateSchema updateSchema) {
    for (Map.Entry<String, List<Column>> entry : addedColumnsToParent.entrySet()) {
      String parent = entry.getKey();
      List<Column> addedColumns = entry.getValue();

      for (Column addedColumn : addedColumns) {
        if (addedColumn.isRequired()) {
          updateSchema.addRequiredColumn(
              parent, addedColumn.getName(), addedColumn.getType(), addedColumn.getDoc());
        } else {
          updateSchema.addColumn(
              parent, addedColumn.getName(), addedColumn.getType(), addedColumn.getDoc());
        }
      }
    }
  }

  void removeColumns(UpdateSchema updateSchema) {
    for (String removedColumn : removedColumns) {
      updateSchema.deleteColumn(removedColumn);
    }
  }

  void modifyColumns(UpdateSchema updateSchema) {
    columnsMarkedAsRequired.forEach(updateSchema::requireColumn);
    columnsMarkedAsOptional.forEach(updateSchema::makeColumnOptional);
    updatedColumnTypes.forEach(
        (columnName, columnType) -> updateSchema.updateColumn(columnName, columnType.getType()));
  }

  void addPartitionColumns(UpdatePartitionSpec updatePartitionSpec) {
    for (PartitionColumn partitionColumn : addedPartitionColumns) {
      updatePartitionSpec.addField(partitionColumn.getName(), partitionColumn.getTerm());
    }
  }

  void removePartitionColumns(UpdatePartitionSpec updatePartitionSpec, PartitionSpec existingSpec) {
    for (String partitionColumnName : removedPartitionColumns) {
      existingSpec.fields().stream()
          .filter(
              field ->
                  availablePartitionColumnSuffixes()
                      .map(suffix -> partitionColumnName + suffix)
                      .anyMatch(field.name()::equals))
          .forEach(
              field -> {
                updatePartitionSpec.removeField(field.name());
              });
    }
  }

  void addProperties(UpdateProperties updateProperties) {
    for (Map.Entry<String, String> entry : addedTableProperties.entrySet()) {
      updateProperties.set(entry.getKey(), entry.getValue());
    }
  }

  void removeProperties(UpdateProperties updateProperties) {
    for (String key : removedTableProperties) {
      updateProperties.remove(key);
    }
  }

  public int getOrder() {
    return _order;
  }

  public String getDescription() {
    return _description;
  }
}
