/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.action

import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.common.xcontent.ToXContent
import com.colasoft.opensearch.common.xcontent.XContentBuilder
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.step.snapshot.AttemptSnapshotStep
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.step.snapshot.WaitForSnapshotStep
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Action
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Step
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class SnapshotAction(
    val repository: String,
    val snapshot: String,
    index: Int
) : Action(name, index) {

    companion object {
        const val name = "snapshot"
        const val REPOSITORY_FIELD = "repository"
        const val SNAPSHOT_FIELD = "snapshot"
    }

    private val attemptSnapshotStep = AttemptSnapshotStep(this)
    private val waitForSnapshotStep = WaitForSnapshotStep(this)
    private val steps = listOf(attemptSnapshotStep, waitForSnapshotStep)

    @Suppress("ReturnCount")
    override fun getStepToExecute(context: StepContext): Step {
        // If stepMetaData is null, return the first step
        val stepMetaData = context.metadata.stepMetaData ?: return attemptSnapshotStep

        // If the current step has completed, return the next step
        if (stepMetaData.stepStatus == Step.StepStatus.COMPLETED) {
            return when (stepMetaData.name) {
                AttemptSnapshotStep.name -> waitForSnapshotStep
                else -> attemptSnapshotStep
            }
        }

        return when (stepMetaData.name) {
            AttemptSnapshotStep.name -> attemptSnapshotStep
            else -> waitForSnapshotStep
        }
    }

    override fun getSteps(): List<Step> = steps

    override fun populateAction(builder: XContentBuilder, params: ToXContent.Params) {
        builder.startObject(type)
        builder.field(REPOSITORY_FIELD, repository)
        builder.field(SNAPSHOT_FIELD, snapshot)
        builder.endObject()
    }

    override fun populateAction(out: StreamOutput) {
        out.writeString(repository)
        out.writeString(snapshot)
        out.writeInt(actionIndex)
    }
}
