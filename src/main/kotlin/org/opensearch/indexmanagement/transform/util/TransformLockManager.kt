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

package com.colasoft.opensearch.indexmanagement.transform.util

import org.apache.logging.log4j.LogManager
import com.colasoft.opensearch.action.bulk.BackoffPolicy
import com.colasoft.opensearch.common.unit.TimeValue
import com.colasoft.opensearch.indexmanagement.opensearchapi.retry
import com.colasoft.opensearch.indexmanagement.opensearchapi.suspendUntil
import com.colasoft.opensearch.indexmanagement.transform.model.Transform
import com.colasoft.opensearch.indexmanagement.transform.settings.TransformSettings
import com.colasoft.opensearch.indexmanagement.util.OpenForTesting
import com.colasoft.opensearch.jobscheduler.spi.JobExecutionContext
import com.colasoft.opensearch.jobscheduler.spi.LockModel
import java.time.Instant

/**
 * Takes and releases the locks during the transform job execution
 * TODO - refactor and use for other features
 */
@OpenForTesting
class TransformLockManager(
    private val transformJob: Transform,
    val context: JobExecutionContext
) {
    private val logger = LogManager.getLogger(javaClass)

    private val exponentialBackoffPolicy = BackoffPolicy.exponentialBackoff(
        TimeValue.timeValueMillis(TransformSettings.DEFAULT_RENEW_LOCK_RETRY_DELAY),
        TransformSettings.DEFAULT_RENEW_LOCK_RETRY_COUNT
    )
    var lock: LockModel? = null
        protected set

    fun lockExpirationInSeconds() = lock?.let { it.lockTime.epochSecond + it.lockDurationSeconds - Instant.now().epochSecond }
    /**
     * Util method to attempt to get the lock on the requested scheduled job using the backoff policy.
     * If failed to acquire the lock using backoff policy will return a null lock otherwise returns acquired lock.
     * Acquiring the lock will fail if there is already running transform job execution for the same transform (transform with the same transform id).
     */
    suspend fun acquireLockForScheduledJob() {
        try {
            // acquireLock will attempt to create the lock index if needed and then read/create a lock. This is purely for internal purposes
            // and should not need the role's context to run
            exponentialBackoffPolicy.retry(logger) {
                lock = context.lockService.suspendUntil { acquireLock(transformJob, context, it) }
            }
        } catch (e: Exception) {
            logger.error("Failed to acquireLock for job ${transformJob.name}", e)
        }
    }

    /**
     * Util method to attempt to renew the requested lock using the backoff policy.
     * If failed to renew the lock using backoff policy will return a null lock otherwise returns renewed lock.
     */
    suspend fun renewLockForScheduledJob(): LockModel? {
        var updatedLock: LockModel? = null
        try {
            exponentialBackoffPolicy.retry(logger) {
                updatedLock = context.lockService.suspendUntil { renewLock(lock, it) }
            }
        } catch (e: Exception) {
            logger.warn("Failed trying to renew lock on $lock, releasing the existing lock", e)
        }

        if (updatedLock == null) {
            releaseLockForScheduledJob()
        }
        lock = updatedLock
        return lock
    }

    /**
     * Util method to attempt to release the requested lock.
     * Returns a boolean of the success of the release lock
     */
    suspend fun releaseLockForScheduledJob(): Boolean {
        var released = false
        try {
            released = context.lockService.suspendUntil { release(lock, it) }
            if (!released) {
                logger.warn("Could not release lock for job ${lock!!.jobId}")
            }
        } catch (e: Exception) {
            logger.error("Failed to release lock for job ${lock!!.jobId}", e)
        }
        return released
    }

    /**
     * Renews the lock if the previous search request of the transform job execution was greater than 10 minutes
     * and if lock expires in less than 20 minutes.
     * Prevents transform job execution timeout in such way by renewing the transform lock, taking into account the time spent for previous search.
     * Prevents the situation of setting the timeout period for the search request (based on lock expiration)
     * that is less than time spent for previous search.
     *
     * @param timeSpentOnSearch - time that is spent when transform does a search
     */
    suspend fun renewLockForLongSearch(timeSpentOnSearch: Long) {
        // If the request was longer than 10 minutes and lock expires in less than 20 minutes, renew the lock just in case
        if (timeSpentOnSearch > TIMEOUT_UPPER_BOUND_IN_SECONDS && lockExpirationInSeconds() ?: 0 < MAXIMUM_LOCK_EXPIRATION_IN_SECONDS
        ) {
            this.renewLockForScheduledJob()
        }
    }

    companion object {
        private const val TIMEOUT_UPPER_BOUND_IN_SECONDS = 600
        private const val MAXIMUM_LOCK_EXPIRATION_IN_SECONDS = 1200
    }
}
