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

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.extension

import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.core.xcontent.ToXContent
import com.colasoft.opensearch.core.xcontent.XContentBuilder
import com.colasoft.opensearch.core.xcontent.XContentParser
import com.colasoft.opensearch.common.xcontent.XContentParserUtils
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Action
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.ActionParser
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.Step
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData

class SampleCustomActionParser : ActionParser() {
    override fun fromStreamInput(sin: StreamInput): Action {
        val someInt = sin.readInt()
        val index = sin.readInt()
        return SampleCustomAction(someInt, index)
    }

    override fun fromXContent(xcp: XContentParser, index: Int): Action {
        var someInt: Int? = null

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, xcp.currentToken(), xcp)
        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
            val fieldName = xcp.currentName()
            xcp.nextToken()

            when (fieldName) {
                SampleCustomAction.SOME_INT_FIELD -> someInt = xcp.intValue()
                else -> throw IllegalArgumentException("Invalid field: [$fieldName] found in SampleCustomAction.")
            }
        }
        return SampleCustomAction(someInt = requireNotNull(someInt) { "SomeInt field must be specified" }, index)
    }

    override fun getActionType(): String {
        return SampleCustomAction.name
    }
    class SampleCustomAction(val someInt: Int, index: Int) : Action(name, index) {

        private val sampleCustomStep = SampleCustomStep()
        private val steps = listOf(sampleCustomStep)

        override fun getStepToExecute(context: StepContext): Step = sampleCustomStep

        override fun getSteps(): List<Step> = steps

        override fun populateAction(builder: XContentBuilder, params: ToXContent.Params) {
            builder.startObject(type)
            builder.field(SOME_INT_FIELD, someInt)
            builder.endObject()
        }

        override fun populateAction(out: StreamOutput) {
            out.writeInt(someInt)
            out.writeInt(actionIndex)
        }

        companion object {
            const val name = "some_custom_action"
            const val SOME_INT_FIELD = "some_int_field"
        }
    }
    class SampleCustomStep : Step(name) {
        private var stepStatus = StepStatus.STARTING

        override suspend fun execute(): Step {
            stepStatus = StepStatus.COMPLETED
            return this
        }

        override fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData {
            return currentMetadata.copy(
                stepMetaData = StepMetaData(name, getStepStartTime(currentMetadata).toEpochMilli(), stepStatus),
                transitionTo = null,
                info = null
            )
        }

        override fun isIdempotent(): Boolean = true

        companion object {
            const val name = "some_custom_step"
        }
    }
}
