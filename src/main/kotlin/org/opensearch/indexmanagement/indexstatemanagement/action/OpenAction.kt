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

import com.colasoft.opensearch.indexmanagement.indexstatemanagement.step.open.AttemptOpenStep
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Action
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Step
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class OpenAction(
    index: Int
) : Action(name, index) {

    companion object {
        const val name = "open"
    }
    private val attemptOpenStep = AttemptOpenStep()
    private val steps = listOf(attemptOpenStep)

    override fun getStepToExecute(context: StepContext): Step {
        return attemptOpenStep
    }

    override fun getSteps(): List<Step> = steps
}
