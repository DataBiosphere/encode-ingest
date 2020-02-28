package org.broadinstitute.monster.encode.extraction

import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}

/** Entry-point for the Encode pipeline's Docker image. */
object ExtractionPipeline extends ScioApp[Args] {
  override def pipelineBuilder: PipelineBuilder[Args] =
    new ExtractionPipelineBuilder(EncodeClient.apply)
}
