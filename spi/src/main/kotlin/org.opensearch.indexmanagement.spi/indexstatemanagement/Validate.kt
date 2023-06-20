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

import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.common.io.stream.Writeable
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.monitor.jvm.JvmService
import java.util.Locale

abstract class Validate(
    val settings: Settings,
    val clusterService: ClusterService,
    val jvmService: JvmService
) {

    var validationStatus = ValidationStatus.PASSED
    var validationMessage: String? = "Starting Validation"

    abstract fun execute(indexName: String): Validate

    enum class ValidationStatus(val status: String) : Writeable {
        PASSED("passed"),
        RE_VALIDATING("re_validating"),
        FAILED("failed");

        override fun toString(): String {
            return status
        }

        override fun writeTo(out: StreamOutput) {
            out.writeString(status)
        }

        companion object {
            fun read(streamInput: StreamInput): ValidationStatus {
                return valueOf(streamInput.readString().uppercase(Locale.ROOT))
            }
        }
    }
}
