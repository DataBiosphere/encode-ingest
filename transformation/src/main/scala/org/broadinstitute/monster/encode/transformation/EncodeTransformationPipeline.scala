package org.broadinstitute.monster.encode.transformation

import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}

/** Entry-point for the Encode Transformation pipeline's Docker image. */
object EncodeTransformationPipeline extends ScioApp[Args] {

  override def pipelineBuilder: PipelineBuilder[Args] =
    EncodeTransformationPipelineBuilder
}
