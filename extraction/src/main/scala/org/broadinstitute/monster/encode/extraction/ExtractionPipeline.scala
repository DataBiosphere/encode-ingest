package org.broadinstitute.monster.encode.extraction

import Args.tupleParser
import org.broadinstitute.monster.common.{PipelineBuilder, ScioApp}

/** Entry-point for the Encode pipeline's Docker image. */
object ExtractionPipeline extends ScioApp[Args] {

  override val pipelineBuilder: PipelineBuilder[Args] =
    // Batch size of 64 seems to work well in practice.
    new ExtractionPipelineBuilder(64L, EncodeClient.apply)
}
