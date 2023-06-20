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

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.opensearchapi

import com.colasoft.opensearch.Version
import com.colasoft.opensearch.action.admin.indices.rollover.RolloverInfo
import com.colasoft.opensearch.cluster.metadata.IndexMetadata
import com.colasoft.opensearch.test.OpenSearchTestCase

class ExtensionsTests : OpenSearchTestCase() {

    fun `test getting oldest rollover time`() {
        val noRolloverMetadata = IndexMetadata
            .Builder("foo-index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build()

        assertNull(noRolloverMetadata.getOldestRolloverTime())
        val oldest = RolloverInfo("bar-alias", emptyList(), 17L)

        val metadata = IndexMetadata
            .Builder(noRolloverMetadata)
            .putRolloverInfo(RolloverInfo("foo-alias", emptyList(), 42L))
            .putRolloverInfo(oldest)
            .putRolloverInfo(RolloverInfo("baz-alias", emptyList(), 134345L))
            .build()

        assertEquals("Did not get the oldest rollover time", oldest.time, metadata.getOldestRolloverTime()?.toEpochMilli())
    }
}
