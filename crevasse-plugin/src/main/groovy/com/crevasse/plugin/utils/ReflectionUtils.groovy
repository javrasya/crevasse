package com.crevasse.plugin.utils

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import org.gradle.api.file.FileCollection

class ReflectionUtils {
    static Class<? extends SpecificRecord> loadClass(String className, FileCollection classpath) {
        final URL[] urls = classpath.getFiles().stream()
                .map(file -> {
                    try {
                        return file.toURI().toURL();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .distinct()
                .toArray(URL[]::new);

        URLClassLoader urlClassLoader = new URLClassLoader(urls);
        return urlClassLoader.loadClass(className) as Class<? extends SpecificRecord>;
    }


    static Schema getSchema(Class<? extends SpecificRecord> clazz) {
        try {
            return (new Schema.Parser()).parse(clazz.getDeclaredMethod("getClassSchema").invoke(null).toString())
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Could not get schema for class ${clazz.name}", e)
        }
    }
}
