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

package com.colasoft.opensearch.indexmanagement.rollup.util

import com.colasoft.opensearch.Version
import com.colasoft.opensearch.client.Client
import com.colasoft.opensearch.cluster.metadata.IndexMetadata
import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.io.stream.NamedWriteableRegistry
import com.colasoft.opensearch.common.regex.Regex
import com.colasoft.opensearch.common.settings.IndexScopedSettings
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.common.settings.SettingsModule
import com.colasoft.opensearch.common.util.BigArrays
import com.colasoft.opensearch.core.xcontent.NamedXContentRegistry
import com.colasoft.opensearch.env.Environment
import com.colasoft.opensearch.index.Index
import com.colasoft.opensearch.index.IndexSettings
import com.colasoft.opensearch.index.mapper.MapperService
import com.colasoft.opensearch.index.query.QueryShardContext
import com.colasoft.opensearch.index.similarity.SimilarityService
import com.colasoft.opensearch.indices.IndicesModule
import com.colasoft.opensearch.indices.analysis.AnalysisModule
import com.colasoft.opensearch.plugins.MapperPlugin
import com.colasoft.opensearch.plugins.PluginsService
import com.colasoft.opensearch.script.ScriptService
import java.time.Instant

/**
 * Creates QueryShardContext object which is used in QueryStringQuery rewrite.
 * We need this because we have to use QueryStringQueryParser class which requires QueryShardContext as parameter
 */
object QueryShardContextFactory {
    lateinit var client: Client
    lateinit var clusterService: ClusterService
    lateinit var scriptService: ScriptService
    lateinit var xContentRegistry: NamedXContentRegistry
    lateinit var namedWriteableRegistry: NamedWriteableRegistry
    lateinit var environment: Environment

    @Suppress("LongParameterList")
    fun init(
        client: Client,
        clusterService: ClusterService,
        scriptService: ScriptService,
        xContentRegistry: NamedXContentRegistry,
        namedWriteableRegistry: NamedWriteableRegistry,
        environment: Environment
    ) {
        this.client = client
        this.clusterService = clusterService
        this.scriptService = scriptService
        this.xContentRegistry = xContentRegistry
        this.namedWriteableRegistry = namedWriteableRegistry
        this.environment = environment
    }

    private fun getIndexSettingsAndMetadata(indexName: String?): Triple<Index?, Settings?, IndexMetadata?> {
        val index: Index?
        val indexSettings: Settings?
        val indexMetadata = clusterService.state().metadata.index(indexName)
            ?: throw IllegalArgumentException("Can't find IndexMetadata for index: [$indexName]")
        index = indexMetadata.index
        indexSettings = indexMetadata.settings
        return Triple(index, indexSettings, indexMetadata)
    }

    fun createShardContext(indexName: String?): QueryShardContext {
        val (index, indexSettings, indexMetadata) = getIndexSettingsAndMetadata(indexName)
        val nodeSettings = Settings.builder()
            .put("node.name", "dummyNodeName")
            .put(Environment.PATH_HOME_SETTING.key, environment.tmpFile())
            .build()
        val pluginsService =
            PluginsService(nodeSettings, null, null, null, listOf())
        val additionalSettings = pluginsService.pluginSettings
        val settingsModule = SettingsModule(
            nodeSettings,
            additionalSettings,
            pluginsService.pluginSettingsFilter, emptySet()
        )
        val indexScopedSettings: IndexScopedSettings = settingsModule.indexScopedSettings
        val idxSettings = newIndexSettings(index, indexSettings, indexScopedSettings)
        val indicesModule = IndicesModule(pluginsService.filterPlugins(MapperPlugin::class.java))
        val mapperRegistry = indicesModule.mapperRegistry
        val analysisModule = AnalysisModule(environment, emptyList())
        val indexAnalyzers = analysisModule.analysisRegistry.build(idxSettings)
        val similarityService = SimilarityService(idxSettings, null, emptyMap())
        val mapperService = MapperService(
            idxSettings,
            indexAnalyzers,
            xContentRegistry,
            similarityService,
            mapperRegistry,
            { createShardContext(null) },
            { false },
            scriptService
        )
        // In order to be able to call toQuery method on QueryBuilder, we need to setup mappings in MapperService
        mapperService.merge("_doc", indexMetadata?.mapping()?.source(), MapperService.MergeReason.MAPPING_UPDATE)

        return QueryShardContext(
            0,
            idxSettings,
            BigArrays.NON_RECYCLING_INSTANCE,
            null,
            null,
            mapperService,
            null,
            scriptService,
            xContentRegistry,
            namedWriteableRegistry,
            null,
            null,
            { Instant.now().toEpochMilli() },
            null,
            { pattern -> Regex.simpleMatch(pattern, index?.name) },
            { true },
            null
        )
    }

    private fun newIndexSettings(index: Index?, settings: Settings?, indexScopedSettings: IndexScopedSettings?): IndexSettings? {
        val build = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(settings)
            .build()
        val metadata = IndexMetadata.builder(index?.name).settings(build).build()
        return IndexSettings(metadata, Settings.EMPTY, indexScopedSettings)
    }
}
