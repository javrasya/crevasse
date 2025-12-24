package com.crevasse.iceberg.schema

import com.crevasse.iceberg.StructTypeHandler
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Types
import spock.lang.Specification

class ColumnBuilderSpec extends Specification {

    def "PrimitiveColumnBuilder should create column with default nullable"() {
        given:
        def builder = new PrimitiveColumnBuilder("test_col", Types.StringType.get())

        when:
        def column = builder.build()

        then:
        column.name == "test_col"
        column.columnType.typeId == Type.TypeID.STRING
        column.isOptional()
        !column.isRequired()
        column.doc == null
    }

    def "PrimitiveColumnBuilder should support notNullable()"() {
        given:
        def builder = new PrimitiveColumnBuilder("test_col", Types.StringType.get())

        when:
        builder.notNullable()
        def column = builder.build()

        then:
        !column.isOptional()
        column.isRequired()
    }

    def "PrimitiveColumnBuilder should support nullable()"() {
        given:
        def builder = new PrimitiveColumnBuilder("test_col", Types.StringType.get())
        builder.notNullable()  // First set to not nullable

        when:
        builder.nullable()  // Then set to nullable
        def column = builder.build()

        then:
        column.isOptional()
        !column.isRequired()
    }

    def "PrimitiveColumnBuilder should support doc()"() {
        given:
        def builder = new PrimitiveColumnBuilder("test_col", Types.StringType.get())

        when:
        builder.doc("Test documentation")
        def column = builder.build()

        then:
        column.doc == "Test documentation"
    }

    def "PrimitiveColumnBuilder should support fluent chaining"() {
        given:
        def builder = new PrimitiveColumnBuilder("test_col", Types.StringType.get())

        when:
        def column = builder.notNullable().doc("Fluent test").build()

        then:
        column.name == "test_col"
        !column.isOptional()
        column.doc == "Fluent test"
    }

    def "DecimalColumnBuilder should create decimal column with precision and scale"() {
        given:
        def builder = new DecimalColumnBuilder("price", 10, 2)

        when:
        def column = builder.notNullable().build()

        then:
        column.name == "price"
        column.columnType.typeId == Type.TypeID.DECIMAL
        column.isRequired()

        def decimalType = (column.columnType as PrimitiveColumnType).type as Types.DecimalType
        decimalType.precision() == 10
        decimalType.scale() == 2
    }

    def "FixedColumnBuilder should create fixed-length column"() {
        given:
        def builder = new FixedColumnBuilder("hash", 32)

        when:
        def column = builder.build()

        then:
        column.name == "hash"
        column.columnType.typeId == Type.TypeID.FIXED

        def fixedType = (column.columnType as PrimitiveColumnType).type as Types.FixedType
        fixedType.length() == 32
    }

    def "ListColumnBuilder should create list column"() {
        given:
        def elementType = new PrimitiveColumnType(Types.StringType.get())
        def builder = new ListColumnBuilder("tags", elementType)

        when:
        def column = builder.notNullable().build()

        then:
        column.name == "tags"
        column.columnType.typeId == Type.TypeID.LIST
        column.isRequired()

        def listType = column.columnType as ListColumnType
        listType.elementType.typeId == Type.TypeID.STRING
    }

    def "StructColumnBuilder should create struct column with nested fields via closure"() {
        given:
        def builder = new StructColumnBuilder("address")

        when:
        def column = builder.nullable {
            stringCol('street')
            stringCol('city').notNullable()
        }.build()

        then:
        column.name == "address"
        column.columnType.typeId == Type.TypeID.STRUCT
        column.isOptional()

        def structType = column.columnType as StructColumnType
        structType.columns.size() == 2
        structType.columns[0].name == "street"
        structType.columns[0].isOptional()
        structType.columns[1].name == "city"
        structType.columns[1].isRequired()
    }

    def "StructColumnBuilder should support notNullable with closure"() {
        given:
        def builder = new StructColumnBuilder("address")

        when:
        def column = builder.notNullable {
            stringCol('street')
        }.build()

        then:
        column.isRequired()
    }

    def "MapColumnBuilder should create map column with key/value types as arguments"() {
        given:
        def keyType = new PrimitiveColumnType(Types.StringType.get())
        def valueType = new PrimitiveColumnType(Types.IntegerType.get())
        def builder = new MapColumnBuilder("metadata", keyType, valueType)

        when:
        def column = builder.build()

        then:
        column.name == "metadata"
        column.columnType.typeId == Type.TypeID.MAP
        column.isOptional()

        def mapType = column.columnType as MapColumnType
        mapType.keyType.typeId == Type.TypeID.STRING
        mapType.valueType.typeId == Type.TypeID.INTEGER
    }

    def "MapColumnBuilder should support notNullable"() {
        given:
        def keyType = new PrimitiveColumnType(Types.StringType.get())
        def valueType = new PrimitiveColumnType(Types.StringType.get())
        def builder = new MapColumnBuilder("metadata", keyType, valueType)

        when:
        def column = builder.notNullable().build()

        then:
        column.isRequired()
    }

    def "MapColumnBuilder should support doc"() {
        given:
        def keyType = new PrimitiveColumnType(Types.StringType.get())
        def valueType = new PrimitiveColumnType(Types.IntegerType.get())
        def builder = new MapColumnBuilder("metadata", keyType, valueType)

        when:
        def column = builder.notNullable().doc("Map documentation").build()

        then:
        column.isRequired()
        column.doc == "Map documentation"
    }

    def "Column.code() should generate fluent DSL for primitive types"() {
        given:
        def column = new Column("test_col", new PrimitiveColumnType(Types.StringType.get()), false, null)

        when:
        def code = column.code()

        then:
        code == "stringCol('test_col').notNullable()"
    }

    def "Column.code() should not add .notNullable() for nullable columns"() {
        given:
        def column = new Column("test_col", new PrimitiveColumnType(Types.StringType.get()), true, null)

        when:
        def code = column.code()

        then:
        code == "stringCol('test_col')"
    }

    def "Column.code() should include doc() when documentation exists"() {
        given:
        def column = new Column("test_col", new PrimitiveColumnType(Types.StringType.get()), false, "Test doc")

        when:
        def code = column.code()

        then:
        code == "stringCol('test_col').notNullable().doc('Test doc')"
    }

    def "Column.code() should generate fluent DSL for struct types with closure"() {
        given:
        def nestedColumns = [
            new Column("street", new PrimitiveColumnType(Types.StringType.get()), true, null),
            new Column("city", new PrimitiveColumnType(Types.StringType.get()), false, null)
        ]
        def column = new Column("address", new StructColumnType(nestedColumns), true, null)

        when:
        def code = column.code()

        then:
        code.contains("structCol('address').nullable {")
        code.contains("stringCol('street')")
        code.contains("stringCol('city').notNullable()")
    }

    def "Column.code() should generate fluent DSL for map types"() {
        given:
        def keyType = new PrimitiveColumnType(Types.StringType.get())
        def valueType = new PrimitiveColumnType(Types.IntegerType.get())
        def column = new Column("metadata", new MapColumnType(keyType, valueType), false, null)

        when:
        def code = column.code()

        then:
        code == "mapCol('metadata', stringType(), intType()).notNullable()"
    }

    def "Column.code() should generate fluent DSL for nullable map types"() {
        given:
        def keyType = new PrimitiveColumnType(Types.StringType.get())
        def valueType = new PrimitiveColumnType(Types.IntegerType.get())
        def column = new Column("metadata", new MapColumnType(keyType, valueType), true, null)

        when:
        def code = column.code()

        then:
        code == "mapCol('metadata', stringType(), intType())"
    }

    def "Column.code() should generate fluent DSL for list types"() {
        given:
        def elementType = new PrimitiveColumnType(Types.StringType.get())
        def column = new Column("tags", new ListColumnType(elementType), false, null)

        when:
        def code = column.code()

        then:
        code == "listCol('tags', stringType()).notNullable()"
    }

    def "StructTypeHandler should return builders that support fluent chaining"() {
        given:
        def handler = new StructTypeHandler()

        when:
        handler.stringCol('id').notNullable()
        handler.stringCol('name')
        handler.stringCol('email').nullable().doc('User email')
        def columns = handler.getColumns()

        then:
        columns.size() == 3

        columns[0].name == 'id'
        columns[0].isRequired()

        columns[1].name == 'name'
        columns[1].isOptional()

        columns[2].name == 'email'
        columns[2].isOptional()
        columns[2].doc == 'User email'
    }
}
