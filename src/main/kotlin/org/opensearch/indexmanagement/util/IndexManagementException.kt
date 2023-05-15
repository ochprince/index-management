/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement.util

import com.colasoft.opensearch.OpenSearchException
import com.colasoft.opensearch.OpenSearchStatusException
import com.colasoft.opensearch.common.Strings
import com.colasoft.opensearch.common.ValidationException
import com.colasoft.opensearch.index.IndexNotFoundException
import com.colasoft.opensearch.rest.RestStatus
import java.lang.IllegalArgumentException

class IndexManagementException(message: String, val status: RestStatus, ex: Exception) : OpenSearchException(message, ex) {

    override fun status(): RestStatus {
        return status
    }

    companion object {
        @JvmStatic
        fun wrap(ex: Exception): OpenSearchException {

            var friendlyMsg = ex.message as String
            var status = RestStatus.INTERNAL_SERVER_ERROR
            when (ex) {
                is IndexNotFoundException -> {
                    status = ex.status()
                    friendlyMsg = "Configuration index not found"
                }
                is IllegalArgumentException -> {
                    status = RestStatus.BAD_REQUEST
                    friendlyMsg = ex.message as String
                }
                is ValidationException -> {
                    status = RestStatus.BAD_REQUEST
                    friendlyMsg = ex.message as String
                }
                is OpenSearchStatusException -> {
                    status = ex.status()
                    friendlyMsg = ex.message as String
                }
                else -> {
                    if (!Strings.isNullOrEmpty(ex.message)) {
                        friendlyMsg = ex.message as String
                    }
                }
            }

            return IndexManagementException(friendlyMsg, status, Exception("${ex.javaClass.name}: ${ex.message}"))
        }
    }
}
