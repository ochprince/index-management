/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action

import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.addpolicy.AddPolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.changepolicy.ChangePolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy.DeletePolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.explain.ExplainAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPoliciesAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy.IndexPolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.removepolicy.RemovePolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex.RetryFailedManagedIndexAction
import com.colasoft.opensearch.test.OpenSearchTestCase

class ActionTests : OpenSearchTestCase() {
    fun `test add policy action name`() {
        assertNotNull(AddPolicyAction.INSTANCE.name())
        assertEquals(AddPolicyAction.INSTANCE.name(), AddPolicyAction.NAME)
    }

    fun `test remove policy action name`() {
        assertNotNull(RemovePolicyAction.INSTANCE.name())
        assertEquals(RemovePolicyAction.INSTANCE.name(), RemovePolicyAction.NAME)
    }

    fun `test retry failed managed index action name`() {
        assertNotNull(RetryFailedManagedIndexAction.INSTANCE.name())
        assertEquals(RetryFailedManagedIndexAction.INSTANCE.name(), RetryFailedManagedIndexAction.NAME)
    }

    fun `test change policy action name`() {
        assertNotNull(ChangePolicyAction.NAME)
        assertEquals(ChangePolicyAction.INSTANCE.name(), ChangePolicyAction.NAME)
    }

    fun `test index policy action name`() {
        assertNotNull(IndexPolicyAction.NAME)
        assertEquals(IndexPolicyAction.INSTANCE.name(), IndexPolicyAction.NAME)
    }

    fun `test explain action name`() {
        assertNotNull(ExplainAction.NAME)
        assertEquals(ExplainAction.INSTANCE.name(), ExplainAction.NAME)
    }

    fun `test delete policy action name`() {
        assertNotNull(DeletePolicyAction.NAME)
        assertEquals(DeletePolicyAction.INSTANCE.name(), DeletePolicyAction.NAME)
    }

    fun `test get policy action name`() {
        assertNotNull(GetPolicyAction.NAME)
        assertEquals(GetPolicyAction.INSTANCE.name(), GetPolicyAction.NAME)
    }

    fun `test get policies action name`() {
        assertNotNull(GetPoliciesAction.NAME)
        assertEquals(GetPoliciesAction.INSTANCE.name(), GetPoliciesAction.NAME)
    }
}
