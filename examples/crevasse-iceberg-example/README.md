# Crevasse Iceberg Example

A working example demonstrating Crevasse schema migrations with Apache Iceberg.

## Prerequisites

- Java 11+ (Java 21 recommended)
- Gradle 9.x (included via wrapper)

> **Note for Java 23+ users:** The Gradle daemon must run on Java 21 due to Hadoop compatibility. Run the setup command below.

## Quick Start

```bash
# 1. Configure Gradle daemon for Java 21 (required for Java 23+ users)
./gradlew updateDaemonJvm --jvm-version=21

# 2. Generate migration scripts from Avro schema
./gradlew generateMigrationScripts

# 3. Apply migrations to create the Iceberg table
./gradlew applyMigrations
```

## What's Included

```
├── src/main/avro/           # Avro schema definitions
├── migrations/              # Generated migration scripts (after running generateMigrationScripts)
├── database/                # Iceberg warehouse (after running applyMigrations)
├── build.gradle             # Plugin configuration with JitPack
├── settings.gradle          # JitPack repository setup
└── gradle/
    └── gradle-daemon-jvm.properties  # Daemon JVM configuration
```

## Configuration

The example is pre-configured to use:
- **JitPack** for plugin resolution (no local build required)
- **Hadoop catalog** with local filesystem warehouse
- **Java 21 toolchain** for compilation

See `build.gradle` for the full Crevasse configuration.

## Modifying the Schema

1. Edit `src/main/avro/MyRecord1.avdl`
2. Run `./gradlew generateMigrationScripts` to detect changes
3. Review the generated migration in `migrations/`
4. Run `./gradlew applyMigrations` to apply changes

## Using Local Build Instead of JitPack

To test against local changes to Crevasse:

```bash
# From the crevasse root directory
./gradlew publishToMavenLocal

# Then update examples/crevasse-iceberg-example/build.gradle:
# Change: id 'com.crevasse.plugin' version "v0.1.0"
# To:     id 'com.crevasse.plugin' version "1.0-SNAPSHOT"

# And add mavenLocal() to repositories in settings.gradle
```
