![crevasse-logo.png](asssets/crevasse-logo.png)

# Crevasse: Schema Migrations for Lakehouse Table Formats

Crevasse is a **table-format agnostic** schema migration framework for modern lakehouse table formats. Inspired by Django's migration system, it provides a Groovy-based DSL for defining table schema changes and a Gradle plugin to generate and apply migrations.

Write your migrations once, apply them to Apache Iceberg today, and to Hudi or Delta Lake tomorrow.

## Key Features

- **Table Format Agnostic**: Core DSL is independent of the underlying table format
- **Avro-Driven Schema Management**: Define table schemas as Avro schemas, detect drift automatically
- **Automatic Migration Generation**: Generate migration scripts when your Avro schemas change
- **Manual Migration Support**: Write custom migrations using the expressive Groovy DSL
- **Versioned Migrations**: Track applied migrations in table metadata, similar to Django
- **Immutable History**: Once applied, migrations form an immutable history of your table's evolution

---

## Table of Contents

- [Architecture](#architecture)
- [Supported Table Formats](#supported-table-formats)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Schema Representation with Avro](#schema-representation-with-avro)
- [Folder Structure](#folder-structure)
- [Two Modes of Operation](#two-modes-of-operation)
- [Migration DSL Reference](#migration-dsl-reference)
- [Migration Examples](#migration-examples)
- [Applying Migrations](#applying-migrations)
- [Migration State Management](#migration-state-management)
- [Caveats and Limitations](#caveats-and-limitations)
- [Table Format Configuration](#table-format-configuration)

---

## Architecture

Crevasse is designed with a **layered architecture** that separates the migration DSL from table format implementations:

```
┌─────────────────────────────────────────────────────────────┐
│                      Avro Schemas                            │
│              (Source of truth for table structure)           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Migration DSL (Groovy)                    │
│                                                              │
│  • addColumns { stringCol(), structCol(), listCol() ... }   │
│  • removeColumn, modifyColumns                               │
│  • addPartitionColumns { year(), month(), bucket() ... }    │
│  • addProperty, removeProperty                               │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                 Table Format Executors                       │
├───────────────────┬───────────────────┬─────────────────────┤
│   Apache Iceberg  │    Apache Hudi    │     Delta Lake      │
│    (Supported)    │     (Planned)     │      (Planned)      │
└───────────────────┴───────────────────┴─────────────────────┘
```

This means:
- **Migration scripts are portable** across table formats
- **The Groovy DSL** you learn works everywhere
- **Only the executor layer** differs per format

---

## Supported Table Formats

| Table Format | Status | Notes |
|--------------|--------|-------|
| **Apache Iceberg** | Supported | Full support for schema evolution, partitioning, and table properties |
| Apache Hudi | Planned | Future support planned |
| Delta Lake | Planned | Future support planned |

### Apache Iceberg

Crevasse provides full support for Apache Iceberg, including:

- Schema evolution (add/remove/modify columns)
- Partition evolution (identity, time-based, bucket, truncate)
- Table properties management
- Nested types (structs, lists, maps)
- Migration state stored in table properties

#### Supported Iceberg Catalogs

| Catalog Type | Description | Use Case |
|--------------|-------------|----------|
| **Hadoop** | File-system based catalog | Local development, HDFS, S3 (with Hadoop FS) |
| **AWS Glue** | AWS Glue Data Catalog | Production AWS environments, integrates with Athena, EMR, Redshift Spectrum |

#### Planned Iceberg Catalog Support

| Catalog Type | Status |
|--------------|--------|
| Hive Metastore | Planned |
| Nessie | Planned |
| JDBC Catalog | Planned |
| REST Catalog | Planned |

---

## How It Works

Crevasse operates similarly to Django migrations:

| Django | Crevasse |
|--------|----------|
| Python model classes | Avro schemas (.avdl/.avsc) |
| `makemigrations` | `generateMigrationScripts` |
| `migrate` | `applyMigrations` |
| `django_migrations` table | Table metadata (format-specific) |
| Migration files | Groovy scripts (migration_N.groovy) |

---

## Installation

Add the Crevasse plugin to your `build.gradle`:

```groovy
plugins {
    id 'java'
    id 'com.github.davidmc24.gradle.plugin.avro' version '1.9.1'
    id 'com.crevasse.plugin' version '1.0-SNAPSHOT'
}

repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    implementation 'org.apache.avro:avro:1.11.1'
}
```

---

## Quick Start

This example uses Apache Iceberg. Configuration varies by table format.

### 1. Define Your Avro Schema

Create an Avro IDL file at `src/main/avro/users.avdl`:

```avro
@namespace("com.example")
protocol Users {
    record User {
        string id;
        string username;
        string? email;
        long created_at;
    }
}
```

### 2. Configure Crevasse

Add the configuration to your `build.gradle`:

```groovy
crevasse {
    scriptDir = file("./migrations")

    // Iceberg configuration
    iceberg {
        catalogs {
            hadoop {
                name "my_catalog"
                warehouse file("./warehouse").path

                schemas {
                    users {
                        table "my_db.users"
                        schemaName "com.example.User"
                    }
                }
            }
        }
    }
}
```

### 3. Generate Migration

```bash
./gradlew generateMigrationScripts
```

This creates `migrations/my_db/users/migration_0.groovy`:

```groovy
package my_db.users

import com.crevasse.iceberg.MigrationBaseScript
import groovy.transform.BaseScript

@BaseScript MigrationBaseScript baseScript

migrate {
    step 0
    description """
        * Initial schema creation
    """

    addColumns {
        stringCol('id', false)
        stringCol('username', false)
        stringCol('email', true)
        longCol('created_at', false)
    }
}
```

### 4. Apply Migration

```bash
./gradlew applyMigrations
```

---

## Schema Representation with Avro

Crevasse uses [Apache Avro](https://avro.apache.org/) schemas as the **format-neutral** source of truth for table structure. This provides:

- **Language-neutral schema definition**: Avro schemas can be shared across different systems
- **Rich type system**: Support for primitives, nested records, arrays, maps, and unions
- **Schema evolution**: Avro's compatibility rules work across all table formats

### Avro IDL Example

```avro
@namespace("com.example.analytics")
protocol Events {
    record Event {
        string event_id;
        string event_type;
        long timestamp;

        // Nullable field (maps to optional column)
        string? user_id;

        // Nested record
        record EventMetadata {
            string source;
            string version;
        } metadata;

        // Array of strings
        array<string> tags;

        // Map of properties
        map<string> properties;
    }
}
```

### Type Mappings

| Avro Type | Crevasse DSL | Description |
|-----------|--------------|-------------|
| `string` | `stringCol()` | Variable-length string |
| `int` | `intCol()` | 32-bit integer |
| `long` | `longCol()` | 64-bit integer |
| `float` | `floatCol()` | 32-bit float |
| `double` | `doubleCol()` | 64-bit double |
| `boolean` | `boolCol()` | Boolean |
| `bytes` | `binaryCol()` | Variable-length binary |
| `record` | `structCol()` | Nested structure |
| `array<T>` | `listCol()` | List/array |
| `map<T>` | `mapCol()` | Key-value map |
| `T?` (union with null) | `*Col(name, true)` | Optional column |

---

## Folder Structure

Crevasse organizes migration scripts in a hierarchical folder structure:

```
migrations/
├── database_name/
│   ├── table_name/
│   │   ├── migration_0.groovy    # Initial schema
│   │   ├── migration_1.groovy    # First change
│   │   └── migration_2.groovy    # Second change
│   └── another_table/
│       └── migration_0.groovy
└── analytics_db/
    └── events/
        ├── migration_0.groovy
        └── migration_1.groovy
```

### Naming Convention

- **Pattern**: `migration_{step}.groovy`
- **Step numbers**: Start at 0 and increment
- **Execution order**: Determined by the `step` number inside the script, not the filename

---

## Two Modes of Operation

### Mode 1: Automatic Migration Generation

When your Avro schema changes, Crevasse detects the drift and generates a migration script:

```bash
# After modifying your Avro schema
./gradlew generateMigrationScripts
```

**How drift detection works:**

1. Crevasse loads all existing migration scripts for a table
2. It "replays" them to build the current schema state
3. It compares this state with your Avro schema
4. It generates a new migration with the differences

**Example workflow:**

```avro
// Original schema
record User {
    string id;
    string name;
}

// Updated schema (added email field)
record User {
    string id;
    string name;
    string? email;  // New field
}
```

Running `generateMigrationScripts` creates:

```groovy
migrate {
    step 1
    description """
        * Add column 'email'
    """
    addColumns {
        stringCol('email', true)
    }
}
```

### Mode 2: Manual Migration Scripts

For operations that cannot be expressed in Avro (partitions, table properties) or for complex migrations, write scripts manually:

```groovy
package my_db.users

import com.crevasse.iceberg.MigrationBaseScript
import groovy.transform.BaseScript

@BaseScript MigrationBaseScript baseScript

migrate {
    step 2
    description """
        * Add partitioning by year
        * Add table ownership metadata
    """

    addPartitionColumns {
        year("created_at")
    }

    addProperty "owner", "data-platform-team"
    addProperty "pii", "true"
}
```

---

## Migration DSL Reference

The migration DSL is **table-format agnostic**. The same syntax works across all supported formats.

### Basic Structure

Every migration script follows this structure:

```groovy
package database_name.table_name

import com.crevasse.iceberg.MigrationBaseScript
import groovy.transform.BaseScript

@BaseScript MigrationBaseScript baseScript

migrate {
    step N              // Migration step number (0, 1, 2, ...)
    description """
        * Description of changes
    """

    // Operations go here
}
```

### Column Types

#### Primitive Types

```groovy
addColumns {
    // String types
    stringCol('name', false)                    // Required string
    stringCol('nickname', true)                 // Optional string
    stringCol('bio', true, 'User biography')    // With documentation

    // Numeric types
    intCol('age', true)                         // 32-bit integer
    longCol('timestamp', false)                 // 64-bit integer
    floatCol('score', true)                     // 32-bit float
    doubleCol('amount', false)                  // 64-bit double
    decimalCol('price', false, 10, 2)           // Decimal(precision, scale)

    // Boolean
    boolCol('is_active', false)

    // Date/Time types
    dateCol('birth_date', true)                 // Date without time
    timeCol('start_time', true)                 // Time without date
    timestampCol('created_at', false)           // Timestamp without timezone
    timestampWithZoneCol('updated_at', true)    // Timestamp with UTC timezone

    // Binary types
    binaryCol('avatar', true)                   // Variable-length binary
    fixedCol('checksum', false, 32)             // Fixed-length binary (32 bytes)
    uuidCol('correlation_id', true)             // UUID
}
```

#### Complex Types

```groovy
addColumns {
    // Struct (nested record)
    structCol('address', true) {
        stringCol('street', false)
        stringCol('city', false)
        stringCol('country', false)
        stringCol('postal_code', true)
    }

    // List/Array
    listCol('tags', true, stringType())
    listCol('scores', false, intType())

    // List of structs
    listCol('addresses', true, structType {
        stringCol('street', false)
        stringCol('city', false)
    })

    // Map
    mapCol('metadata', true) {
        key(stringType())
        value(stringType())
    }

    // Map with struct values
    mapCol('settings', true) {
        key(stringType())
        value(structType {
            stringCol('value', false)
            boolCol('encrypted', false)
        })
    }

    // Deeply nested structures
    structCol('profile', true) {
        stringCol('bio', true)
        listCol('interests', true, stringType())
        mapCol('social_links', true) {
            key(stringType())
            value(stringType())
        }
    }
}
```

### Column Operations

```groovy
migrate {
    step 1
    description "Modify columns"

    // Add new columns
    addColumns {
        stringCol('new_field', true)
    }

    // Remove columns
    removeColumn "deprecated_field"
    removeColumn "old_column"

    // Modify existing columns
    modifyColumns {
        // Make an optional column required
        requireColumn "user_id"

        // Make a required column optional
        makeColumnOptional "legacy_field"

        // Change column type (primitive types only)
        updateColumnType "counter", longType()  // int -> long
    }

    // Add fields to existing struct
    addColumnsToParent("address") {
        stringCol('apartment', true)
        stringCol('floor', true)
    }
}
```

### Partition Operations

```groovy
migrate {
    step 2
    description "Configure partitioning"

    addPartitionColumns {
        // Identity partition (use column value as-is)
        ref("region")

        // Time-based partitions
        year("created_at")      // Partition by year
        month("created_at")     // Partition by month
        day("event_date")       // Partition by day
        hour("event_time")      // Partition by hour

        // Hash partitioning
        bucket("user_id", 32)   // 32 hash buckets

        // String truncation
        truncate("category", 8) // First 8 characters
    }

    // Remove partition columns
    removePartitionColumn "old_partition"
}
```

### Table Properties

```groovy
migrate {
    step 3
    description "Set table properties"

    // Add single property
    addProperty "owner", "analytics-team"
    addProperty "pii", "true"

    // Add multiple properties
    addProperties([
        "write.format.default": "parquet",
        "write.parquet.compression-codec": "zstd",
        "commit.retry.num-retries": "10"
    ])

    // Remove properties
    removeProperty "deprecated_setting"
}
```

---

## Migration Examples

### Example 1: Initial Table Creation

```groovy
migrate {
    step 0
    description """
        * Create users table with basic fields
    """

    addColumns {
        stringCol('user_id', false)
        stringCol('username', false)
        stringCol('email', true)
        timestampCol('created_at', false)
        boolCol('is_verified', false)
    }
}
```

### Example 2: Adding Nested Structures

```groovy
migrate {
    step 1
    description """
        * Add user profile with preferences
    """

    addColumns {
        structCol('profile', true) {
            stringCol('display_name', true)
            stringCol('bio', true)
            stringCol('avatar_url', true)

            structCol('preferences', true) {
                boolCol('email_notifications', true)
                boolCol('dark_mode', true)
                stringCol('language', true)
                stringCol('timezone', true)
            }
        }
    }
}
```

### Example 3: Adding Collections

```groovy
migrate {
    step 2
    description """
        * Add tags and metadata collections
    """

    addColumns {
        // Simple list
        listCol('tags', true, stringType())

        // List of complex objects
        listCol('login_history', true, structType {
            timestampWithZoneCol('timestamp', false)
            stringCol('ip_address', false)
            stringCol('user_agent', true)
        })

        // Key-value metadata
        mapCol('custom_attributes', true) {
            key(stringType())
            value(stringType())
        }
    }
}
```

### Example 4: Schema Evolution

```groovy
migrate {
    step 3
    description """
        * Remove deprecated fields
        * Rename conceptually (add new, remove old)
        * Update column types
    """

    // Remove old columns
    removeColumn "legacy_status"
    removeColumn "temp_field"

    // Add replacement columns
    addColumns {
        stringCol('status', false)
        intCol('status_code', false)
    }

    // Widen numeric types
    modifyColumns {
        updateColumnType "view_count", longType()  // int -> long
    }
}
```

### Example 5: Partitioning and Properties

```groovy
migrate {
    step 4
    description """
        * Configure time-based partitioning
        * Set table metadata
    """

    addPartitionColumns {
        year("created_at")
        month("created_at")
        bucket("user_id", 16)
    }

    addProperties([
        "owner": "user-service-team",
        "pii": "true",
        "retention.days": "365"
    ])
}
```

### Example 6: Complex Real-World Migration

```groovy
migrate {
    step 5
    description """
        * Add e-commerce order tracking
        * Include nested line items
        * Configure for analytics queries
    """

    addColumns {
        stringCol('order_id', false)
        stringCol('customer_id', false)

        structCol('shipping_address', false) {
            stringCol('name', false)
            stringCol('street', false)
            stringCol('city', false)
            stringCol('state', true)
            stringCol('postal_code', false)
            stringCol('country', false)
        }

        listCol('line_items', false, structType {
            stringCol('product_id', false)
            stringCol('product_name', false)
            intCol('quantity', false)
            decimalCol('unit_price', false, 10, 2)
            decimalCol('total_price', false, 10, 2)
            mapCol('attributes', true) {
                key(stringType())
                value(stringType())
            }
        })

        decimalCol('subtotal', false, 12, 2)
        decimalCol('tax', false, 12, 2)
        decimalCol('total', false, 12, 2)

        stringCol('status', false)
        timestampWithZoneCol('ordered_at', false)
        timestampWithZoneCol('shipped_at', true)
    }

    addPartitionColumns {
        day("ordered_at")
        bucket("customer_id", 32)
    }
}
```

---

## Applying Migrations

### Run All Pending Migrations

```bash
./gradlew applyMigrations
```

### How Migration Execution Works

1. **Load existing migrations** from the configured `scriptDir`
2. **Read current state** from table metadata (format-specific storage)
3. **Filter pending migrations** where `step > current_state`
4. **Execute in order** by step number
5. **Update state** after each successful migration

### What Happens During Migration

For each migration, Crevasse:

1. Parses the Groovy script
2. Builds a transaction with all schema changes
3. Applies changes atomically to the table
4. Updates migration state in table metadata
5. Records timestamp of last migration

---

## Migration State Management

### Where State is Stored

Migration state is stored in **table metadata**. The exact storage location depends on the table format:

| Table Format | State Storage |
|--------------|---------------|
| Apache Iceberg | Table properties (`crevasse.migration.state`) |
| Apache Hudi | Table properties (planned) |
| Delta Lake | Table properties (planned) |

**Example (Iceberg):**
```
crevasse.migration.state = 5
crevasse.migration.last-applied-at = 2024-01-15T10:30:00Z
```

This approach:
- **Co-locates state with data**: No separate state database needed
- **Atomic updates**: State changes are part of the table commit
- **Portable**: State travels with the table

### Immutability of Applied Migrations

**Once a migration is applied, it should never be modified.**

This is similar to Django migrations:

- Applied migrations are part of your table's history
- Modifying an applied migration will cause drift between environments
- To "undo" a change, create a new migration that reverses it

```groovy
// DON'T: Modify migration_1.groovy after it's applied

// DO: Create migration_2.groovy to reverse the change
migrate {
    step 2
    description "Revert: Remove field added in step 1"

    removeColumn "field_added_in_step_1"
}
```

---

## Caveats and Limitations

### What Cannot Be Auto-Generated

Certain table features **cannot be encoded in Avro schemas**, so they must be manually added to migration scripts:

| Feature | Reason | Solution |
|---------|--------|----------|
| **Partition columns** | No Avro equivalent | Add manually with `addPartitionColumns` |
| **Sort order** | No Avro equivalent | Use table format APIs directly |
| **Table properties** | Format-specific | Add manually with `addProperty` |
| **Column comments** | Avro has `doc`, but optional | Add as column parameter |
| **Default values** | Format-specific | Not currently supported |

### Generated Migration Review

**Always review auto-generated migrations before applying!**

The generator may not perfectly capture your intent:

```groovy
// Generated migration might add columns in different order
// You may want to reorder or group them logically

// Generated migration won't include:
// - Partitioning strategy
// - Table properties
// - Performance optimizations
```

### Recommended Workflow

1. **Generate** the base migration from Avro changes
2. **Review** the generated script
3. **Enhance** with partitions, properties, etc.
4. **Test** in a development environment
5. **Apply** to production

```groovy
// Auto-generated base
migrate {
    step 1
    description "Add user preferences"

    addColumns {
        stringCol('timezone', true)
        stringCol('language', true)
    }
}

// Enhanced version (after review)
migrate {
    step 1
    description """
        * Add user preferences
        * Configure for efficient timezone queries
    """

    addColumns {
        stringCol('timezone', true)
        stringCol('language', true)
    }

    // Manually added
    addPartitionColumns {
        ref("timezone")
    }

    addProperty "pii", "false"
}
```

### Type Change Limitations

Some type changes may not be allowed depending on the table format:

- Cannot narrow types (long -> int)
- Cannot change between incompatible types (string -> int)
- Struct field changes follow the same rules

---

## Table Format Configuration

### Apache Iceberg

#### Hadoop Catalog (Local/HDFS)

```groovy
crevasse {
    scriptDir = file("./migrations")

    iceberg {
        catalogs {
            hadoop {
                name "local_catalog"
                warehouse file("./warehouse").path

                schemas {
                    users {
                        table "my_db.users"
                        schemaName "com.example.User"
                    }
                }
            }
        }
    }
}
```

#### AWS Glue Catalog

```groovy
crevasse {
    scriptDir = file("./migrations")

    iceberg {
        catalogs {
            glue {
                name "production_catalog"
                warehouse "s3://my-bucket/warehouse"

                schemas {
                    events {
                        table "analytics.events"
                        schemaName "com.example.Event"
                    }
                }
            }
        }
    }
}
```

#### Multiple Catalogs

```groovy
crevasse {
    scriptDir = file("./migrations")

    iceberg {
        catalogs {
            hadoop {
                name "dev_catalog"
                warehouse file("./dev-warehouse").path

                schemas {
                    users {
                        table "dev_db.users"
                        schemaName "com.example.User"
                    }
                }
            }

            glue {
                name "prod_catalog"
                warehouse "s3://prod-bucket/warehouse"

                schemas {
                    users {
                        table "prod_db.users"
                        schemaName "com.example.User"
                    }
                }
            }
        }
    }
}
```

#### Ignoring Columns

Exclude columns from migration generation:

```groovy
schemas {
    users {
        table "my_db.users"
        schemaName "com.example.User"
        ignoredColumns "internal_id", "temp_field"
    }
}
```

#### Disabling Schema Processing

```groovy
schemas {
    legacyTable {
        table "my_db.legacy"
        schemaName "com.example.Legacy"
        enabled false  // Skip this schema
    }
}
```

---

## License

Apache License 2.0
