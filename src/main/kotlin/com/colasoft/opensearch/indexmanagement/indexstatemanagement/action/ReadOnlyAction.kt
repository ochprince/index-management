/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.action

import com.colasoft.opensearch.indexmanagement.indexstatemanagement.step.readonly.SetReadOnlyStep
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Action
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Step
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class ReadOnlyAction(
    index: Int
) : Action(name, index) {

    companion object {
        const val name = "read_only"
    }
    private val setReadOnlyStep = SetReadOnlyStep()
    private val steps = listOf(setReadOnlyStep)

    override fun getStepToExecute(context: StepContext): Step {
        return setReadOnlyStep
    }

    override fun getSteps(): List<Step> = steps
}
