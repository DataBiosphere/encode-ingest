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

`sbt "encode-transformation-pipeline / runMain org.broadinstitute.monster.encode.transformation.TransformationPipeline --inputPrefix=<extraction dir> --outputPrefix=<output dir>"`

Development of Argo changes requires deployment to the DEV environment as documented in the [monster-deploy repo](https://github.com/broadinstitute/monster-deploy/)

## Build and deploy code chages from branch


#### *Do you have a new Macbook Pro with the M1 chip?*

1. If so, clone this repo `https://github.com/DataBiosphere/ingest-utils.git`
1. Check out the branch `ah_m1arch
`

### Steps in encode-ingest repo

1. Before you commit changes, make sure the build occurs without error `sbt compile` and that the tests run without errors `sbt test`. 
	
	*The build may reformat some of your files. Make sure to do a diff and add any changes to your git staging area.*
1. Commit changes to local branch `git commit -m "<comment>"` 
1. Create a version tag for the branch `git tag v1.0.<new-number>`. For example `v1.0.120`
	
	*You can run a `git log	` to see the previous version number or go to the actions tab for the repo `https://github.com/DataBiosphere/encode-ingest/actions` to see the previous version built. It is very important the the version format is exact. No `.` between the `v` and the `1`.*
1. Now push the branch and tag to the remote `git push origin : v1.0.<x>`

	*You can also push your branch to the remote and then separately push the tags to the remote `git push --tags`*
	
	*This will start 2 build actions. If the build fails, you can look at the details for errors or just run a local `sbt compile`. In some cases you may have forgotten to commit formatting changes done automatically by `sbt compile`. Just add them to your staging area and start with #2 above.*

