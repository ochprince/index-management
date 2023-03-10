/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package com.colasoft.opensearch.indexmanagement.snapshotmanagement.settings

import com.colasoft.opensearch.common.settings.Setting

@Suppress("UtilityClassWithPublicConstructor")
class SnapshotManagementSettings {

    companion object {
        val FILTER_BY_BACKEND_ROLES: Setting<Boolean> = Setting.boolSetting(
            "plugins.snapshot_management.filter_by_backend_roles",
            false,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )
    }
}
