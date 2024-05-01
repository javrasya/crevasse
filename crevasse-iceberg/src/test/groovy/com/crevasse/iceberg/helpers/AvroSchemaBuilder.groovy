package com.crevasse.iceberg.helpers

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder

class AvroSchemaBuilder {
    SchemaBuilder.FieldAssembler<Schema> fieldAssembler

    AvroSchemaBuilder(String schemaName) {
        this.fieldAssembler = SchemaBuilder.builder()
                .record(schemaName)
                .namespace("com.crevasse.iceberg")
                .fields()
    }

    AvroSchemaBuilder(SchemaBuilder.FieldAssembler<Schema> fieldAssembler) {
        this.fieldAssembler = fieldAssembler
    }

    def requiredString(String name) {
        fieldAssembler = fieldAssembler.requiredString(name)
    }

    def optionalString(String name) {
        fieldAssembler = fieldAssembler.name(name).type().unionOf().nullType().and().stringType().endUnion().nullDefault()
    }

    def requiredInt(String name) {
        fieldAssembler = fieldAssembler.requiredInt(name)
    }

    def optionalInt(String name) {
        fieldAssembler = fieldAssembler.name(name).type().unionOf().nullType().and().intType().endUnion().nullDefault()
    }

    def requiredLong(String name) {
        fieldAssembler = fieldAssembler.requiredLong(name)
    }

    def optionalLong(String name) {
        fieldAssembler = fieldAssembler.name(name).type().unionOf().nullType().and().longType().endUnion().nullDefault()
    }

    def requiredFloat(String name) {
        fieldAssembler = fieldAssembler.requiredFloat(name)
    }

    def optionalFloat(String name) {
        fieldAssembler = fieldAssembler.name(name).type().unionOf().nullType().and().floatType().endUnion().nullDefault()
    }

    def requiredDouble(String name) {
        fieldAssembler = fieldAssembler.requiredDouble(name)
    }

    def optionalDouble(String name) {
        fieldAssembler = fieldAssembler.name(name).type().unionOf().nullType().and().doubleType().endUnion().nullDefault()
    }

    def requiredStruct(String name, String schemaName, @DelegatesTo(AvroSchemaBuilder) Closure closure) {
        def structFieldAssembler = SchemaBuilder.builder()
                .record(schemaName)
                .namespace("com.crevasse.iceberg")
                .fields()
        def structBuilder = new AvroSchemaBuilder(structFieldAssembler)
        closure.setDelegate(structBuilder)
        closure.setResolveStrategy(Closure.DELEGATE_ONLY)
        closure.call()
        fieldAssembler = fieldAssembler.name(name).type(structBuilder.build()).noDefault()
    }

    def optionalStruct(String name, String schemaName, @DelegatesTo(AvroSchemaBuilder) Closure closure) {
        def structFieldAssembler = SchemaBuilder.builder()
                .record(schemaName)
                .namespace("com.crevasse.iceberg")
                .fields()
        def structBuilder = new AvroSchemaBuilder(structFieldAssembler)
        closure.setDelegate(structBuilder)
        closure.setResolveStrategy(Closure.DELEGATE_ONLY)
        closure.call()
        fieldAssembler = fieldAssembler.name(name).type().unionOf().nullType().and().type(structBuilder.build()).endUnion().nullDefault()
    }

    def requiredMapOfPrimitive(String name, Schema.Type type) {
        fieldAssembler = fieldAssembler.name(name).type().map().values(Schema.create(type)).noDefault()
    }

    def optionalMapOfPrimitive(String name, Schema.Type type) {
        fieldAssembler = fieldAssembler.name(name).type().unionOf().nullType().and().map().values(Schema.create(type)).endUnion().nullDefault()
    }

    def requiredMapOfStruct(String name, String valueSchemaName, @DelegatesTo(AvroSchemaBuilder) Closure closure) {
        def structFieldAssembler = SchemaBuilder.builder()
                .record(valueSchemaName)
                .namespace("com.crevasse.iceberg")
                .fields()
        def structBuilder = new AvroSchemaBuilder(structFieldAssembler)
        closure.setDelegate(structBuilder)
        closure.setResolveStrategy(Closure.DELEGATE_ONLY)
        closure.call()
        fieldAssembler = fieldAssembler.name(name).type().map().values(structBuilder.build()).noDefault()
    }

    def optionalMapOfStruct(String name, String valueSchemaName, @DelegatesTo(AvroSchemaBuilder) Closure closure) {
        def structFieldAssembler = SchemaBuilder.builder()
                .record(valueSchemaName)
                .namespace("com.crevasse.iceberg")
                .fields()
        def structBuilder = new AvroSchemaBuilder(structFieldAssembler)
        closure.setDelegate(structBuilder)
        closure.setResolveStrategy(Closure.DELEGATE_ONLY)
        closure.call()
        fieldAssembler = fieldAssembler.name(name).type().unionOf().nullType().and().map().values(structBuilder.build()).endUnion().nullDefault()
    }

    def requiredArrayOfPrimitive(String name, Schema.Type type) {
        fieldAssembler = fieldAssembler.name(name).type().array().items(Schema.create(type)).noDefault()
    }

    def optionalArrayOfPrimitive(String name, Schema.Type type) {
        fieldAssembler = fieldAssembler.name(name).type().unionOf().nullType().and().array().items(Schema.create(type)).endUnion().nullDefault()
    }

    def requiredArrayOfStruct(String name, String valueSchemaName, @DelegatesTo(AvroSchemaBuilder) Closure closure) {
        def structFieldAssembler = SchemaBuilder.builder()
                .record(valueSchemaName)
                .namespace("com.crevasse.iceberg")
                .fields()
        def structBuilder = new AvroSchemaBuilder(structFieldAssembler)
        closure.setDelegate(structBuilder)
        closure.setResolveStrategy(Closure.DELEGATE_ONLY)
        closure.call()
        fieldAssembler = fieldAssembler.name(name).type().array().items(structBuilder.build()).noDefault()
    }

    def optionalArrayOfStruct(String name, String valueSchemaName, @DelegatesTo(AvroSchemaBuilder) Closure closure) {
        def structFieldAssembler = SchemaBuilder.builder()
                .record(valueSchemaName)
                .namespace("com.crevasse.iceberg")
                .fields()
        def structBuilder = new AvroSchemaBuilder(structFieldAssembler)
        closure.setDelegate(structBuilder)
        closure.setResolveStrategy(Closure.DELEGATE_ONLY)
        closure.call()
        fieldAssembler = fieldAssembler.name(name).type().unionOf().nullType().and().array().items(structBuilder.build()).endUnion().nullDefault()
    }

    def build() {
        return fieldAssembler.endRecord();
    }
}
