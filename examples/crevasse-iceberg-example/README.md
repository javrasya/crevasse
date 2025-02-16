## Pre-requisites

Crevasse plugin should be installed in your local maven repository. To install the plugin, run the following command from the root directory of the project:

```shell

pushd ../../
./gradlew publishToMavenLocal
popd
```


## Generate migrations


To generate migrations, run the following command from the root directory of the project:

```shell
./gradlew generateMigrationScripts --stacktrace
```


## Apply migrations

To apply migrations, run the following command from the root directory of the project:

```shell
./gradlew applyMigrations --stacktrace
```
