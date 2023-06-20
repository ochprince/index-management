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

import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.core.xcontent.ToXContent
import com.colasoft.opensearch.core.xcontent.XContentBuilder
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.step.replicacount.AttemptReplicaCountStep
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Action
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Step
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class ReplicaCountAction(
    val numOfReplicas: Int,
    index: Int
) : Action(name, index) {

    init {
        require(numOfReplicas >= 0) { "ReplicaCountAction number_of_replicas value must be a non-negative number" }
    }

    private val attemptReplicaCountStep = AttemptReplicaCountStep(this)
    private val steps = listOf(attemptReplicaCountStep)

    override fun getStepToExecute(context: StepContext): Step {
        return attemptReplicaCountStep
    }

    override fun getSteps(): List<Step> = steps

    override fun populateAction(builder: XContentBuilder, params: ToXContent.Params) {
        builder.startObject(type)
        builder.field(NUMBER_OF_REPLICAS_FIELD, numOfReplicas)
        builder.endObject()
    }

    override fun populateAction(out: StreamOutput) {
        out.writeInt(numOfReplicas)
        out.writeInt(actionIndex)
    }

    companion object {
        const val NUMBER_OF_REPLICAS_FIELD = "number_of_replicas"
        const val name = "replica_count"
    }
}
