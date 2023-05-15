/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.action

import com.colasoft.opensearch.indexmanagement.indexstatemanagement.step.readwrite.SetReadWriteStep
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Action
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Step
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class ReadWriteAction(
    index: Int
) : Action(name, index) {

    companion object {
        const val name = "read_write"
    }

    private val setReadWriteStep = SetReadWriteStep()
    private val steps = listOf(setReadWriteStep)

    override fun getStepToExecute(context: StepContext): Step {
        return setReadWriteStep
    }

    override fun getSteps(): List<Step> = steps
}
