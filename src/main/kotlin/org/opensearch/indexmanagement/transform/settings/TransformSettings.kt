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

package com.colasoft.opensearch.indexmanagement.transform.settings

import com.colasoft.opensearch.common.settings.Setting
import com.colasoft.opensearch.common.unit.TimeValue

@Suppress("UtilityClassWithPublicConstructor")
class TransformSettings {

    companion object {
        const val DEFAULT_RENEW_LOCK_RETRY_COUNT = 3
        const val DEFAULT_RENEW_LOCK_RETRY_DELAY = 1000L

        val TRANSFORM_JOB_SEARCH_BACKOFF_COUNT: Setting<Int> = Setting.intSetting(
            "plugins.transform.internal.search.backoff_count",
            5,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val TRANSFORM_JOB_SEARCH_BACKOFF_MILLIS: Setting<TimeValue> = Setting.positiveTimeSetting(
            "plugins.transform.internal.search.backoff_millis",
            TimeValue.timeValueMillis(1000),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val TRANSFORM_JOB_INDEX_BACKOFF_COUNT: Setting<Int> = Setting.intSetting(
            "plugins.transform.internal.index.backoff_count",
            5,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val TRANSFORM_JOB_INDEX_BACKOFF_MILLIS: Setting<TimeValue> = Setting.positiveTimeSetting(
            "plugins.transform.internal.index.backoff_millis",
            TimeValue.timeValueMillis(1000),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val TRANSFORM_CIRCUIT_BREAKER_ENABLED: Setting<Boolean> = Setting.boolSetting(
            "plugins.transform.circuit_breaker.enabled",
            true,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val TRANSFORM_CIRCUIT_BREAKER_JVM_THRESHOLD: Setting<Int> = Setting.intSetting(
            "plugins.transform.circuit_breaker.jvm.threshold",
            85,
            0,
            100,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )
    }
}
