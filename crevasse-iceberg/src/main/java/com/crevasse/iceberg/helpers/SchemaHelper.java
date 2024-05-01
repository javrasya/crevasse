package com.crevasse.iceberg.helpers;

import com.crevasse.iceberg.SchemaDifferences;
import com.crevasse.iceberg.schema.*;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

public class SchemaHelper implements Serializable {

  public static SchemaDifferences calculateSchemaDifferences(
      Types.StructType oldIcebergSchema,
      Types.StructType newIcebergSchema,
      List<String> ignoredColumns) {
    return calculateSchemaDifferences(oldIcebergSchema, newIcebergSchema, ignoredColumns, true);
  }

  public static SchemaDifferences calculateSchemaDifferences(
      Types.StructType oldIcebergSchema,
      Types.StructType newIcebergSchema,
      List<String> ignoredColumns,
      boolean captureRemovedAndModified) {
    final SchemaDifferences schemaDifferences = new SchemaDifferences();
    if (oldIcebergSchema == null) {
      for (Types.NestedField field : newIcebergSchema.fields()) {
        schemaDifferences.addField(toColumn(field));
      }
    } else {
      if (captureRemovedAndModified) {
        oldIcebergSchema.fields().stream()
            .filter(f -> !ignoredColumns.contains(f.name()))
            .forEach(
                oldField -> {
                  Types.NestedField fieldInNewSchema = newIcebergSchema.field(oldField.name());
                  if (fieldInNewSchema == null) {
                    schemaDifferences.removeField(oldField.name());
                  } else if (fieldInNewSchema.isOptional() != oldField.isOptional()) {
                    if (fieldInNewSchema.isOptional())
                      schemaDifferences.markColumnAsOptional(oldField.name());
                    else schemaDifferences.requireColumn(oldField.name());
                  } else if (!oldField.type().isNestedType()
                      && !fieldInNewSchema.type().isNestedType()
                      && !fieldInNewSchema.type().typeId().equals(oldField.type().typeId())) {
                    if (!TypeUtil.isPromotionAllowed(
                        oldField.type(), fieldInNewSchema.type().asPrimitiveType())) {
                      throw new IllegalStateException(
                          "The column '"
                              + oldField.name()
                              + "' can not be promoted from '"
                              + oldField.type()
                              + "' to '"
                              + fieldInNewSchema.type()
                              + "'");
                    }
                    schemaDifferences.updateColumnType(
                        oldField.name(), toColumnType(fieldInNewSchema.type()));
                  } else if (oldField.type().isStructType()
                      && fieldInNewSchema.type().isStructType()) {
                    SchemaDifferences innerSchemaDifference =
                        calculateSchemaDifferences(
                            oldField.type().asStructType(),
                            fieldInNewSchema.type().asStructType(),
                            ignoredColumns,
                            false);
                    List<Column> addedFields = innerSchemaDifference.getAddedColumns();
                    addedFields.forEach(
                        addedField ->
                            schemaDifferences.addFieldToParent(oldField.name(), addedField));
                  } else if (oldField.type().isMapType()
                      && fieldInNewSchema.type().isMapType()
                      && oldField.type().asMapType().valueType().isStructType()
                      && fieldInNewSchema.type().asMapType().valueType().isStructType()) {
                    SchemaDifferences innerSchemaDifference =
                        calculateSchemaDifferences(
                            oldField.type().asMapType().valueType().asStructType(),
                            fieldInNewSchema.type().asMapType().valueType().asStructType(),
                            ignoredColumns,
                            false);
                    List<Column> addedFields = innerSchemaDifference.getAddedColumns();
                    addedFields.forEach(
                        addedField ->
                            schemaDifferences.addFieldToParent(oldField.name(), addedField));
                  } else if (oldField.type().isListType()
                      && fieldInNewSchema.type().isListType()
                      && oldField.type().asListType().elementType().isStructType()
                      && fieldInNewSchema.type().asListType().elementType().isStructType()) {
                    SchemaDifferences innerSchemaDifference =
                        calculateSchemaDifferences(
                            oldField.type().asListType().elementType().asStructType(),
                            fieldInNewSchema.type().asListType().elementType().asStructType(),
                            ignoredColumns,
                            false);
                    List<Column> addedFields = innerSchemaDifference.getAddedColumns();
                    addedFields.forEach(
                        addedField ->
                            schemaDifferences.addFieldToParent(oldField.name(), addedField));
                  }
                });
      }

      newIcebergSchema.fields().stream()
          .filter(f -> !ignoredColumns.contains(f.name()))
          .forEach(
              newField -> {
                if (oldIcebergSchema.field(newField.name()) == null) {
                  schemaDifferences.addField(toColumn(newField));
                }
              });
    }

    return schemaDifferences;
  }

  public static Column toColumn(Types.NestedField field) {
    return new Column(field.name(), toColumnType(field.type()), field.isOptional(), field.doc());
  }

  private static ColumnType toColumnType(Type type) {
    if (type.isPrimitiveType()) {
      return new PrimitiveColumnType(type.asPrimitiveType());
    } else if (type.isMapType()) {
      Types.MapType mapType = type.asMapType();
      return new MapColumnType(toColumnType(mapType.keyType()), toColumnType(mapType.valueType()));
    } else if (type.isListType()) {
      return new ListColumnType(toColumnType(type.asListType().elementType()));
    } else if (type.isStructType()) {
      return new StructColumnType(
          type.asStructType().fields().stream()
              .map(SchemaHelper::toColumn)
              .collect(Collectors.toList()));
    } else {
      throw new IllegalArgumentException("Unsupported type: " + type);
    }
  }
}
