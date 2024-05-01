package com.crevasse.iceberg.schema;

import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.iceberg.types.Type;

@Getter
public class StructColumnType implements ColumnType {
  private final List<Column> columns;

  public StructColumnType(List<Column> columns) {
    this.columns = columns;
  }

  @Override
  public Type.TypeID getTypeId() {
    return Type.TypeID.STRUCT;
  }

  @Override
  public String code() {
    return String.format(
        "structType {\n\t" + "%s\n\t\t" + "\n\t}",
        columns.stream().map(Column::code).collect(Collectors.joining("\n\t\t")));
  }
}
