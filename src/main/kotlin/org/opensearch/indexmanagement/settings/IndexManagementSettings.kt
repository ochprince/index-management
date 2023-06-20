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
package com.colasoft.opensearch.indexmanagement.settings

import com.colasoft.opensearch.common.settings.Setting

@Suppress("UtilityClassWithPublicConstructor")
class IndexManagementSettings {

    companion object {

        val FILTER_BY_BACKEND_ROLES: Setting<Boolean> = Setting.boolSetting(
            "plugins.index_management.filter_by_backend_roles",
            false,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )
    }
}
