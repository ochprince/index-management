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
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.step.indexpriority.AttemptSetIndexPriorityStep
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Action
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Step
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class IndexPriorityAction(
    val indexPriority: Int,
    index: Int
) : Action(name, index) {

    init {
        require(indexPriority >= 0) { "IndexPriorityAction index_priority value must be a non-negative number" }
    }

    private val attemptSetIndexPriorityStep = AttemptSetIndexPriorityStep(this)
    private val steps = listOf(attemptSetIndexPriorityStep)

    override fun getStepToExecute(context: StepContext): Step = attemptSetIndexPriorityStep

    override fun getSteps(): List<Step> = steps

    override fun populateAction(builder: XContentBuilder, params: ToXContent.Params) {
        builder.startObject(type)
        builder.field(INDEX_PRIORITY_FIELD, indexPriority)
        builder.endObject()
    }

    override fun populateAction(out: StreamOutput) {
        out.writeInt(indexPriority)
        out.writeInt(actionIndex)
    }

    companion object {
        const val name = "index_priority"
        const val INDEX_PRIORITY_FIELD = "priority"
    }
}
