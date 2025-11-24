package com.trendyol.quafka.extensions.common.pipelines

/**
 * Configuration options for conditional pipeline branching.
 *
 * Used internally by [PipelineBuilder.map] to create conditional branches in the pipeline.
 * When the condition evaluates to `true`, the envelope is processed through the branch pipeline.
 *
 * @param TEnvelope The type of envelope being processed.
 * @property condition A predicate function to determine if the branch should be taken.
 * @property branch The pipeline to execute if the condition is true.
 *
 * @see PipelineBuilder.map
 * @see MapPipelineMiddleware
 */
data class MapOptions<TEnvelope>(val condition: (TEnvelope) -> Boolean, val branch: Pipeline<TEnvelope>)
