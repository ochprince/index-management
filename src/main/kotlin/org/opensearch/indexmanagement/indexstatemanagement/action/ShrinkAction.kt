/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The ColaSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.action

import com.colasoft.opensearch.action.admin.indices.alias.Alias
import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.common.unit.ByteSizeValue
import com.colasoft.opensearch.core.xcontent.ToXContent
import com.colasoft.opensearch.core.xcontent.XContentBuilder
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.action.NotificationAction.Companion.MUSTACHE
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.step.shrink.AttemptMoveShardsStep
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.step.shrink.AttemptShrinkStep
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.step.shrink.WaitForMoveShardsStep
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.step.shrink.WaitForShrinkStep
import com.colasoft.opensearch.indexmanagement.opensearchapi.aliasesField
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Action
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Step
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import com.colasoft.opensearch.script.Script

@Suppress("LongParameterList")
class ShrinkAction(
    val numNewShards: Int?,
    val maxShardSize: ByteSizeValue?,
    val percentageOfSourceShards: Double?,
    val targetIndexTemplate: Script?,
    val aliases: List<Alias>?,
    val forceUnsafe: Boolean?,
    index: Int
) : Action(name, index) {
    init {
        val numSet = arrayOf(maxShardSize != null, percentageOfSourceShards != null, numNewShards != null).count { it }
        require(numSet == 1) { "Exactly one option specifying the number of shards to shrink to must be used." }

        if (maxShardSize != null) {
            require(maxShardSize.bytes > 0) { "Shrink action maxShardSize must be greater than 0." }
        }
        if (percentageOfSourceShards != null) {
            require(percentageOfSourceShards > 0.0 && percentageOfSourceShards < 1.0) {
                "Percentage of source shards must be between 0.0 and 1.0 exclusively"
            }
        }
        if (numNewShards != null) {
            require(numNewShards > 0) { "Shrink action numNewShards must be greater than 0." }
        }
        if (targetIndexTemplate != null) {
            require(targetIndexTemplate.lang == MUSTACHE) { "Target index name template must be a mustache script" }
        }
    }

    private val attemptMoveShardsStep = AttemptMoveShardsStep(this)
    private val waitForMoveShardsStep = WaitForMoveShardsStep(this)
    private val attemptShrinkStep = AttemptShrinkStep(this)
    private val waitForShrinkStep = WaitForShrinkStep(this)

    private val stepNameToStep: LinkedHashMap<String, Step> = linkedMapOf(
        AttemptMoveShardsStep.name to attemptMoveShardsStep,
        WaitForMoveShardsStep.name to waitForMoveShardsStep,
        AttemptShrinkStep.name to attemptShrinkStep,
        WaitForShrinkStep.name to waitForShrinkStep
    )
    override fun getSteps(): List<Step> = listOf(attemptMoveShardsStep, waitForMoveShardsStep, attemptShrinkStep, waitForShrinkStep)

    @SuppressWarnings("ReturnCount")
    override fun getStepToExecute(context: StepContext): Step {
        val stepMetaData = context.metadata.stepMetaData ?: return attemptMoveShardsStep
        val currentStep = stepMetaData.name

        // If the current step is not from this action, assume it is from another action.
        if (!stepNameToStep.containsKey(currentStep)) return attemptMoveShardsStep

        val currentStepStatus = stepMetaData.stepStatus
        if (currentStepStatus == Step.StepStatus.COMPLETED) {
            return when (currentStep) {
                AttemptMoveShardsStep.name -> waitForMoveShardsStep
                WaitForMoveShardsStep.name -> attemptShrinkStep
                AttemptShrinkStep.name -> waitForShrinkStep
                // We do not expect to ever hit this point, but if we do somehow, starting over is safe.
                else -> attemptMoveShardsStep
            }
        } else if (currentStepStatus == Step.StepStatus.FAILED) {
            // If we failed at any point, retries should start from the beginning
            return attemptMoveShardsStep
        }

        // step not completed, return the same step
        return when (stepMetaData.name) {
            AttemptMoveShardsStep.name -> attemptMoveShardsStep
            WaitForMoveShardsStep.name -> waitForMoveShardsStep
            AttemptShrinkStep.name -> attemptShrinkStep
            WaitForShrinkStep.name -> waitForShrinkStep
            // Again, we don't expect to ever hit this point
            else -> attemptMoveShardsStep
        }
    }

    override fun populateAction(builder: XContentBuilder, params: ToXContent.Params) {
        builder.startObject(type)
        if (numNewShards != null) builder.field(NUM_NEW_SHARDS_FIELD, numNewShards)
        if (maxShardSize != null) builder.field(MAX_SHARD_SIZE_FIELD, maxShardSize.stringRep)
        if (percentageOfSourceShards != null) builder.field(PERCENTAGE_OF_SOURCE_SHARDS_FIELD, percentageOfSourceShards)
        if (targetIndexTemplate != null) builder.field(TARGET_INDEX_TEMPLATE_FIELD, targetIndexTemplate)
        if (aliases != null) { builder.aliasesField(aliases) }
        if (forceUnsafe != null) builder.field(FORCE_UNSAFE_FIELD, forceUnsafe)
        builder.endObject()
    }

    override fun populateAction(out: StreamOutput) {
        out.writeOptionalInt(numNewShards)
        out.writeOptionalWriteable(maxShardSize)
        out.writeOptionalDouble(percentageOfSourceShards)
        out.writeBoolean(targetIndexTemplate != null)
        targetIndexTemplate?.writeTo(out)
        if (aliases != null) {
            out.writeBoolean(true)
            out.writeList(aliases)
        } else {
            out.writeBoolean(false)
        }
        out.writeOptionalBoolean(forceUnsafe)
        out.writeInt(actionIndex)
    }

    companion object {
        const val name = "shrink"
        const val NUM_NEW_SHARDS_FIELD = "num_new_shards"
        const val PERCENTAGE_OF_SOURCE_SHARDS_FIELD = "percentage_of_source_shards"
        const val MAX_SHARD_SIZE_FIELD = "max_shard_size"
        const val TARGET_INDEX_TEMPLATE_FIELD = "target_index_name_template"
        const val ALIASES_FIELD = "aliases"
        const val FORCE_UNSAFE_FIELD = "force_unsafe"
        const val LOCK_SOURCE_JOB_ID = "shrink-node_name"
        fun getSecurityFailureMessage(failure: String) = "Shrink action failed because of missing permissions: $failure"
    }
}
