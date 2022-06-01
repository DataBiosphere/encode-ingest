# ENCODE Ingest

Batch ETL pipeline to mirror ENCODE data into the Terra Data Repository (TDR). See the [architecture documentation](https://github.com/DataBiosphere/encode-ingest/blob/master/ARCHITECTURE.md) for 
further design details.

## Getting Started

Orchestration of the ETL flows in this project is implemented using [Argo Workflows](https://argoproj.github.io/argo-workflows/).

The core extraction and transformation data pipelines are implemented in [Scio](https://spotify.github.io/scio/) on top of Apache Beam.

After cloning the repository, ensure you can compile the code, auto-generate schema classes
and run the test suite from the repository root:

`sbt test`

## Development Process

All development should be done on branches off of the protected `master` branch. After review, merge to `master`
and then follow the instuctions in the [monster-deploy repo](https://github.com/broadinstitute/monster-deploy/)

When modifying the Scio data pipelines, it's possible to run the pipeline locally by invoking the relevant pipeline:

* **Extraction:** 

`sbt "encode-extraction / runMain org.broadinstitute.monster.encode.extraction.ExtractionPipeline  --outputDir=<some local directory>"`
* **Transformation** 

`sbt "encode-transformation-pipeline / runMain org.broadinstitute.monster.encode.transformation.TransformationPipeline --inputPrefix=<extraction dir> --outputPrefix=<output dir>" --fileStorage=<file dir>`

Development of Argo changes requires deployment to the DEV environment as documented in the [monster-deploy repo](https://github.com/broadinstitute/monster-deploy/)
