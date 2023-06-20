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

import com.colasoft.opensearch.indexmanagement.indexstatemanagement.IndexMetadataProvider
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.Transition
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.step.transition.AttemptTransitionStep
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Action
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Step
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext

class TransitionsAction(
    val transitions: List<Transition>,
    val indexMetadataProvider: IndexMetadataProvider
) : Action(name, -1) {

    private val attemptTransitionStep = AttemptTransitionStep(this)
    private val steps = listOf(attemptTransitionStep)

    override fun getSteps(): List<Step> = steps

    override fun getStepToExecute(context: StepContext): Step {
        return attemptTransitionStep
    }

    companion object {
        const val name = "transition"
    }
}
