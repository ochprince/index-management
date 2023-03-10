/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.rollup.model

import com.colasoft.opensearch.common.io.stream.StreamInput
import com.colasoft.opensearch.common.io.stream.StreamOutput
import com.colasoft.opensearch.common.io.stream.Writeable
import com.colasoft.opensearch.common.xcontent.ToXContent
import com.colasoft.opensearch.common.xcontent.ToXContentObject
import com.colasoft.opensearch.common.xcontent.XContentBuilder
import java.io.IOException

data class ExplainRollup(
    val metadataID: String? = null,
    val metadata: RollupMetadata? = null
) : ToXContentObject, Writeable {

    @Throws(IOException::class)
    constructor(sin: StreamInput) : this(
        metadataID = sin.readOptionalString(),
        metadata = if (sin.readBoolean()) RollupMetadata(sin) else null
    )

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        out.writeOptionalString(metadataID)
        out.writeBoolean(metadata != null)
        metadata?.writeTo(out)
    }

    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params): XContentBuilder {
        return builder.startObject()
            .field(Rollup.METADATA_ID_FIELD, metadataID)
            .field(RollupMetadata.ROLLUP_METADATA_TYPE, metadata)
            .endObject()
    }
}
