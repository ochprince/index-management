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

package com.colasoft.opensearch.indexmanagement.indexstatemanagement.model

import com.colasoft.opensearch.common.unit.ByteSizeValue
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomByteSizeValue
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.randomTimeValueObject
import com.colasoft.opensearch.test.OpenSearchTestCase
import kotlin.test.assertFailsWith

class ConditionsTests : OpenSearchTestCase() {

    fun `test supplying more than one transition condition fails`() {
        assertFailsWith(
            IllegalArgumentException::class,
            "Expected IllegalArgumentException for supplying multiple transition conditions"
        ) {
            Conditions(indexAge = randomTimeValueObject(), size = randomByteSizeValue())
        }
    }

    fun `test doc count condition of zero fails`() {
        assertFailsWith(
            IllegalArgumentException::class,
            "Expected IllegalArgumentException for doc count condition less than 1"
        ) {
            Conditions(docCount = 0)
        }
    }

    fun `test size condition of zero fails`() {
        assertFailsWith(
            IllegalArgumentException::class,
            "Expected IllegalArgumentException for size condition less than 1"
        ) {
            Conditions(size = ByteSizeValue.parseBytesSizeValue("0", "size_test"))
        }
    }
}
