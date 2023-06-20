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

package com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement

import org.apache.logging.log4j.Logger
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.common.io.stream.Writeable
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.StepContext
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.StepMetaData
import java.time.Instant
import java.util.Locale

abstract class Step(val name: String, val isSafeToDisableOn: Boolean = true) {

    var context: StepContext? = null
        private set

    fun preExecute(logger: Logger, context: StepContext): Step {
        logger.info("Executing $name for ${context.metadata.index}")
        this.context = context
        return this
    }

    abstract suspend fun execute(): Step

    fun postExecute(logger: Logger): Step {
        logger.info("Finished executing $name for ${context?.metadata?.index}")
        this.context = null
        return this
    }

    abstract fun getUpdatedManagedIndexMetadata(currentMetadata: ManagedIndexMetaData): ManagedIndexMetaData

    abstract fun isIdempotent(): Boolean

    final fun getStepStartTime(metadata: ManagedIndexMetaData): Instant {
        return when {
            metadata.stepMetaData == null -> Instant.now()
            metadata.stepMetaData.name != this.name -> Instant.now()
            // The managed index metadata is a historical snapshot of the metadata and refers to what has happened from the previous
            // execution, so if we ever see it as COMPLETED it means we are always going to be in a new step, this specifically
            // helps with the Transition -> Transition (empty state) sequence which the above do not capture
            metadata.stepMetaData.stepStatus == StepStatus.COMPLETED -> Instant.now()
            else -> Instant.ofEpochMilli(metadata.stepMetaData.startTime)
        }
    }

    final fun getStartingStepMetaData(metadata: ManagedIndexMetaData): StepMetaData = StepMetaData(name, getStepStartTime(metadata).toEpochMilli(), StepStatus.STARTING)

    enum class StepStatus(val status: String) : Writeable {
        STARTING("starting"),
        CONDITION_NOT_MET("condition_not_met"),
        FAILED("failed"),
        COMPLETED("completed");

        override fun toString(): String {
            return status
        }

        override fun writeTo(out: StreamOutput) {
            out.writeString(status)
        }

        companion object {
            fun read(streamInput: StreamInput): StepStatus {
                return valueOf(streamInput.readString().uppercase(Locale.ROOT))
            }
        }
    }
}
