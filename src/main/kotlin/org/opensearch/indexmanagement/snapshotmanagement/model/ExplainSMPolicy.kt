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

package com.colasoft.opensearch.indexmanagement.snapshotmanagement.model

import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.common.io.stream.Writeable
import com.colasoft.opensearch.core.xcontent.ToXContent
import com.colasoft.opensearch.core.xcontent.ToXContentFragment
import com.colasoft.opensearch.core.xcontent.XContentBuilder
import com.colasoft.opensearch.indexmanagement.opensearchapi.optionalField
import java.io.IOException

data class ExplainSMPolicy(
    val metadata: SMMetadata? = null,
    val enabled: Boolean? = null
) : ToXContentFragment, Writeable {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        metadata = if (sin.readBoolean()) SMMetadata(sin) else null,
        enabled = sin.readOptionalBoolean()
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeBoolean(metadata != null)
        metadata?.writeTo(out)
        out.writeOptionalBoolean(enabled)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        metadata?.let {
            builder
                .field(SMMetadata.CREATION_FIELD, it.creation)
                .optionalField(SMMetadata.DELETION_FIELD, it.deletion)
                .field(SMMetadata.POLICY_SEQ_NO_FIELD, it.policySeqNo)
                .field(SMMetadata.POLICY_PRIMARY_TERM_FIELD, it.policyPrimaryTerm)
        }
        return builder.field(SMPolicy.ENABLED_FIELD, enabled)
    }
}
