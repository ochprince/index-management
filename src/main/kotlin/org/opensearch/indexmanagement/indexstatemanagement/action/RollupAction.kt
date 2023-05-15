/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.action

import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.core.xcontent.ToXContent
import com.colasoft.opensearch.core.xcontent.XContentBuilder
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.step.rollup.AttemptCreateRollupJobStep
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.step.rollup.WaitForRollupCompletionStep
import com.colasoft.opensearch.indexmanagement.rollup.model.ISMRollup
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Action
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Step
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class RollupAction(
    val ismRollup: ISMRollup,
    index: Int
) : Action(name, index) {

    companion object {
        const val name = "rollup"
        const val ISM_ROLLUP_FIELD = "ism_rollup"
    }

    private val attemptCreateRollupJobStep = AttemptCreateRollupJobStep(this)
    private val waitForRollupCompletionStep = WaitForRollupCompletionStep()
    private val steps = listOf(attemptCreateRollupJobStep, waitForRollupCompletionStep)

    @Suppress("ReturnCount")
    override fun getStepToExecute(context: StepContext): Step {
        // If stepMetaData is null, return the first step
        val stepMetaData = context.metadata.stepMetaData ?: return attemptCreateRollupJobStep

        // If the current step has completed, return the next step
        if (stepMetaData.stepStatus == Step.StepStatus.COMPLETED) {
            return when (stepMetaData.name) {
                AttemptCreateRollupJobStep.name -> waitForRollupCompletionStep
                else -> attemptCreateRollupJobStep
            }
        }

        return when (stepMetaData.name) {
            AttemptCreateRollupJobStep.name -> attemptCreateRollupJobStep
            else -> waitForRollupCompletionStep
        }
    }

    override fun getSteps(): List<Step> = steps

    override fun populateAction(builder: XContentBuilder, params: ToXContent.Params) {
        builder.startObject(type)
        builder.field(ISM_ROLLUP_FIELD, ismRollup)
        builder.endObject()
    }

    override fun populateAction(out: StreamOutput) {
        ismRollup.writeTo(out)
        out.writeInt(actionIndex)
    }
}
