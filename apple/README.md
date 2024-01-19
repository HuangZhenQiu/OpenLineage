# ASE Open Lineage Flink Libs Release Process

## Run the integration test locally

In Open Lineage Flink integration module, only some basic functionality can be covered by unit tests.
For the schema in dataset, it relies on external schema providers to function. Thus, we need to rely
on integration tests to guarantee the correctness and also validate whether the lineage events
are generated with right format. As Rio haven't supported test container in runtime rdar://92135250, 
we can only run it locally. Thus, please run it before release a new version.

```
cd ./client/java
./gradlew publishToMavenLocal

cd ../../integration/flink
./gradlew integrationTest
```

## Release from Rio
Currently, we release Flink libs from the branch 1.7.1-ase-apple. You may trigger a release by 
using the [Rio pipeline](https://rio.apple.com/projects/pie-ipr-lf-openlineage/pipeline-specs/pie-ipr-lf-openlineage-openlineage-1.7-ase-flink-publish).
