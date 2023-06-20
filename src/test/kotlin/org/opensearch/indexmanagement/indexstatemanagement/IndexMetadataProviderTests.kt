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

package com.colasoft.opensearch.indexmanagement.indexstatemanagement

import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import org.junit.Before
import com.colasoft.opensearch.client.Client
import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.settings.ClusterSettings
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.indexmanagement.IndexManagementPlugin
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.IndexMetadataService
import com.colasoft.opensearch.test.OpenSearchTestCase
import com.colasoft.opensearch.test.rest.OpenSearchRestTestCase

class IndexMetadataProviderTests : OpenSearchTestCase() {

    private val clusterService: ClusterService = mock()
    private val client: Client = mock()
    private val settings: Settings = Settings.EMPTY
    private val services = mutableMapOf<String, IndexMetadataService>()

    @Before
    fun `setup settings`() {
        whenever(clusterService.clusterSettings).doReturn(ClusterSettings(Settings.EMPTY, setOf(ManagedIndexSettings.RESTRICTED_INDEX_PATTERN)))
    }

    fun `test security index and kibana should not be manageable`() {
        val indexEvaluator = IndexMetadataProvider(settings, client, clusterService, services)
        assertTrue("Should not manage security index", indexEvaluator.isUnManageableIndex(".opendistro_security"))
        assertTrue("Should not manage kibana index", indexEvaluator.isUnManageableIndex(".kibana_1"))
        assertTrue("Should not manage kibana index", indexEvaluator.isUnManageableIndex(".kibana"))
        assertTrue("Should not manage kibana index", indexEvaluator.isUnManageableIndex(".kibana_20"))
        assertTrue("Should not manage kibana index", indexEvaluator.isUnManageableIndex(".kibana_022"))
        assertTrue(
            "Should not manage index management config index",
            indexEvaluator.isUnManageableIndex(
                IndexManagementPlugin
                    .INDEX_MANAGEMENT_INDEX
            )
        )
        assertTrue("Should not manage kibana index", indexEvaluator.isUnManageableIndex(".kibana_1242142_user"))

        val randomIndex = OpenSearchRestTestCase.randomAlphaOfLength(OpenSearchRestTestCase.randomIntBetween(1, 20))
        assertFalse("Should manage non kibana and security indices", indexEvaluator.isUnManageableIndex(randomIndex))
    }
}
