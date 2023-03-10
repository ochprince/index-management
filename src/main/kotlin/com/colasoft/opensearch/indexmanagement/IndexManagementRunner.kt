/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement

import org.apache.logging.log4j.LogManager
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.ManagedIndexRunner
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import com.colasoft.opensearch.indexmanagement.rollup.RollupRunner
import com.colasoft.opensearch.indexmanagement.rollup.model.Rollup
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.SMRunner
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import com.colasoft.opensearch.indexmanagement.transform.TransformRunner
import com.colasoft.opensearch.indexmanagement.transform.model.Transform
import com.colasoft.opensearch.jobscheduler.spi.JobExecutionContext
import com.colasoft.opensearch.jobscheduler.spi.ScheduledJobParameter
import com.colasoft.opensearch.jobscheduler.spi.ScheduledJobRunner

object IndexManagementRunner : ScheduledJobRunner {

    private val logger = LogManager.getLogger(javaClass)

    override fun runJob(job: ScheduledJobParameter, context: JobExecutionContext) {
        when (job) {
            is ManagedIndexConfig -> ManagedIndexRunner.runJob(job, context)
            is Rollup -> RollupRunner.runJob(job, context)
            is Transform -> TransformRunner.runJob(job, context)
            is SMPolicy -> SMRunner.runJob(job, context)
            else -> {
                val errorMessage = "Invalid job type, found ${job.javaClass.simpleName} with id: ${context.jobId}"
                logger.error(errorMessage)
                throw IllegalArgumentException(errorMessage)
            }
        }
    }
}
