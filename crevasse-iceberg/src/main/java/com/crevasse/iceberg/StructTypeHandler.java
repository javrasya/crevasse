package com.crevasse.iceberg;

import com.crevasse.iceberg.schema.*;
import groovy.lang.Closure;
import groovy.lang.DelegatesTo;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.ToString;
import org.apache.iceberg.types.Types;

@Getter
@ToString
public class StructTypeHandler implements WithColumnTypesUtilities {
  private final List<Column> columns = new ArrayList<>();

  public void stringCol(String name, boolean isOptional) {
    this.stringCol(name, isOptional, null);
  }

  public void stringCol(String name, boolean isOptional, String doc) {
    columns.add(new Column(name, new PrimitiveColumnType(Types.StringType.get()), isOptional, doc));
  }

  public void intCol(String name, boolean isOptional) {
    this.intCol(name, isOptional, null);
  }

  public void intCol(String name, boolean isOptional, String doc) {
    columns.add(
        new Column(name, new PrimitiveColumnType(Types.IntegerType.get()), isOptional, doc));
  }

  public void longCol(String name, boolean isOptional) {
    this.longCol(name, isOptional, null);
  }

  public void longCol(String name, boolean isOptional, String doc) {
    columns.add(new Column(name, new PrimitiveColumnType(Types.LongType.get()), isOptional, doc));
  }

  public void floatCol(String name, boolean isOptional) {
    this.floatCol(name, isOptional, null);
  }

  public void floatCol(String name, boolean isOptional, String doc) {
    columns.add(new Column(name, new PrimitiveColumnType(Types.FloatType.get()), isOptional, doc));
  }

  public void doubleCol(String name, boolean isOptional) {
    this.doubleCol(name, isOptional, null);
  }

  public void doubleCol(String name, boolean isOptional, String doc) {
    columns.add(new Column(name, new PrimitiveColumnType(Types.DoubleType.get()), isOptional, doc));
  }

  public void boolCol(String name, boolean isOptional) {
    this.boolCol(name, isOptional, null);
  }

  public void boolCol(String name, boolean isOptional, String doc) {
    columns.add(
        new Column(name, new PrimitiveColumnType(Types.BooleanType.get()), isOptional, doc));
  }

  public void dateCol(String name, boolean isOptional) {
    this.dateCol(name, isOptional, null);
  }

  public void dateCol(String name, boolean isOptional, String doc) {
    columns.add(new Column(name, new PrimitiveColumnType(Types.DateType.get()), isOptional, doc));
  }

  public void timestampCol(String name, boolean isOptional) {
    this.timestampCol(name, isOptional, null);
  }

  public void timestampCol(String name, boolean isOptional, String doc) {
    columns.add(
        new Column(
            name, new PrimitiveColumnType(Types.TimestampType.withoutZone()), isOptional, doc));
  }

  public void timestampWithZoneCol(String name, boolean isOptional) {
    this.timestampWithZoneCol(name, isOptional, null);
  }

  public void timestampWithZoneCol(String name, boolean isOptional, String doc) {
    columns.add(
        new Column(name, new PrimitiveColumnType(Types.TimestampType.withZone()), isOptional, doc));
  }

  public void timeCol(String name, boolean isOptional) {
    this.timeCol(name, isOptional, null);
  }

  public void timeCol(String name, boolean isOptional, String doc) {
    columns.add(new Column(name, new PrimitiveColumnType(Types.TimeType.get()), isOptional, doc));
  }

  public void decimalCol(String name, boolean isOptional, int precision, int scale) {
    this.decimalCol(name, isOptional, precision, scale, null);
  }

  public void decimalCol(String name, boolean isOptional, int precision, int scale, String doc) {
    columns.add(
        new Column(
            name,
            new PrimitiveColumnType(Types.DecimalType.of(precision, scale)),
            isOptional,
            doc));
  }

  public void fixedCol(String name, boolean isOptional, int length) {
    this.fixedCol(name, isOptional, length, null);
  }

  public void fixedCol(String name, boolean isOptional, int length, String doc) {
    columns.add(
        new Column(
            name, new PrimitiveColumnType(Types.FixedType.ofLength(length)), isOptional, doc));
  }

  public void binaryCol(String name, boolean isOptional) {
    this.binaryCol(name, isOptional, null);
  }

  public void binaryCol(String name, boolean isOptional, String doc) {
    columns.add(new Column(name, new PrimitiveColumnType(Types.BinaryType.get()), isOptional, doc));
  }

  public void uuidCol(String name, boolean isOptional) {
    this.uuidCol(name, isOptional, null);
  }

  public void uuidCol(String name, boolean isOptional, String doc) {
    columns.add(new Column(name, new PrimitiveColumnType(Types.UUIDType.get()), isOptional, doc));
  }

  public void listCol(String name, boolean isOptional, ColumnType elementType) {
    this.listCol(name, isOptional, null, elementType);
  }

  public void listCol(String name, boolean isOptional, String doc, ColumnType elementType) {
    columns.add(new Column(name, new ListColumnType(elementType), isOptional, doc));
  }

  public void mapCol(
      String name, boolean isOptional, @DelegatesTo(MapTypeHandler.class) Closure closure) {
    this.mapCol(name, isOptional, null, closure);
  }

  public void mapCol(
      String name,
      boolean isOptional,
      String doc,
      @DelegatesTo(MapTypeHandler.class) Closure closure) {
    MapTypeHandler mapTypeHandler = new MapTypeHandler();
    closure.setDelegate(mapTypeHandler);
    closure.setResolveStrategy(Closure.DELEGATE_ONLY);
    closure.call();

    columns.add(
        new Column(
            name,
            new MapColumnType(mapTypeHandler.getKeyType(), mapTypeHandler.getValueType()),
            isOptional,
            doc));
  }

  public void structCol(
      String name, boolean isOptional, @DelegatesTo(StructTypeHandler.class) Closure closure) {
    this.structCol(name, isOptional, null, closure);
  }

  public void structCol(
      String name,
      boolean isOptional,
      String doc,
      @DelegatesTo(StructTypeHandler.class) Closure closure) {
    StructTypeHandler structTypeHandler = new StructTypeHandler();
    closure.setDelegate(structTypeHandler);
    closure.setResolveStrategy(Closure.DELEGATE_ONLY);
    closure.call();

    columns.add(
        new Column(
            name,
            new StructColumnType(new ArrayList<>(structTypeHandler.getColumns())),
            isOptional,
            doc));
  }

  @Getter
  public static class MapTypeHandler implements WithColumnTypesUtilities {
    private ColumnType keyType;
    private ColumnType valueType;

    public void key(ColumnType columnType) {
      keyType = columnType;
    }

    public void value(ColumnType columnType) {
      valueType = columnType;
    }
  }
}
