/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.colasoft.opensearch.indexmanagement

import org.apache.logging.log4j.LogManager
import com.colasoft.opensearch.action.ActionRequest
import com.colasoft.opensearch.action.ActionResponse
import com.colasoft.opensearch.action.support.ActionFilter
import com.colasoft.opensearch.client.Client
import com.colasoft.opensearch.cluster.metadata.IndexNameExpressionResolver
import com.colasoft.opensearch.cluster.node.DiscoveryNodes
import com.colasoft.opensearch.cluster.service.ClusterService
import com.colasoft.opensearch.common.component.Lifecycle
import com.colasoft.opensearch.common.component.LifecycleComponent
import com.colasoft.opensearch.common.component.LifecycleListener
import com.colasoft.opensearch.common.inject.Inject
import com.colasoft.opensearch.common.io.stream.NamedWriteableRegistry
import com.colasoft.opensearch.common.settings.ClusterSettings
import com.colasoft.opensearch.common.settings.IndexScopedSettings
import com.colasoft.opensearch.common.settings.Setting
import com.colasoft.opensearch.common.settings.Settings
import com.colasoft.opensearch.common.settings.SettingsFilter
import com.colasoft.opensearch.common.util.concurrent.ThreadContext
import com.colasoft.opensearch.common.xcontent.NamedXContentRegistry
import com.colasoft.opensearch.common.xcontent.XContentParser.Token
import com.colasoft.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import com.colasoft.opensearch.env.Environment
import com.colasoft.opensearch.env.NodeEnvironment
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.DefaultIndexMetadataService
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.ExtensionStatusChecker
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.ISMActionsParser
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.IndexMetadataProvider
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.IndexStateManagementHistory
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.ManagedIndexCoordinator
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.ManagedIndexRunner
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.MetadataService
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.PluginVersionSweepCoordinator
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.SkipExecution
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.ManagedIndexConfig
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.model.Policy
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.resthandler.RestAddPolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.resthandler.RestChangePolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.resthandler.RestDeletePolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.resthandler.RestExplainAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.resthandler.RestGetPolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.resthandler.RestIndexPolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.resthandler.RestRemovePolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.resthandler.RestRetryFailedManagedIndexAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.settings.LegacyOpenDistroManagedIndexSettings
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.addpolicy.AddPolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.addpolicy.TransportAddPolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.changepolicy.ChangePolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.changepolicy.TransportChangePolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy.DeletePolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.deletepolicy.TransportDeletePolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.explain.ExplainAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.explain.TransportExplainAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPoliciesAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.GetPolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.TransportGetPoliciesAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.getpolicy.TransportGetPolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy.IndexPolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.indexpolicy.TransportIndexPolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.ManagedIndexAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.managedIndex.TransportManagedIndexAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.removepolicy.RemovePolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.removepolicy.TransportRemovePolicyAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex.RetryFailedManagedIndexAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.retryfailedmanagedindex.TransportRetryFailedManagedIndexAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.TransportUpdateManagedIndexMetaDataAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.transport.action.updateindexmetadata.UpdateManagedIndexMetaDataAction
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.util.DEFAULT_INDEX_TYPE
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.validation.ActionValidation
import com.colasoft.opensearch.indexmanagement.indexstatemanagement.migration.ISMTemplateService
import com.colasoft.opensearch.indexmanagement.refreshanalyzer.RefreshSearchAnalyzerAction
import com.colasoft.opensearch.indexmanagement.refreshanalyzer.RestRefreshSearchAnalyzerAction
import com.colasoft.opensearch.indexmanagement.refreshanalyzer.TransportRefreshSearchAnalyzerAction
import com.colasoft.opensearch.indexmanagement.rollup.RollupIndexer
import com.colasoft.opensearch.indexmanagement.rollup.RollupMapperService
import com.colasoft.opensearch.indexmanagement.rollup.RollupMetadataService
import com.colasoft.opensearch.indexmanagement.rollup.RollupRunner
import com.colasoft.opensearch.indexmanagement.rollup.RollupSearchService
import com.colasoft.opensearch.indexmanagement.rollup.action.delete.DeleteRollupAction
import com.colasoft.opensearch.indexmanagement.rollup.action.delete.TransportDeleteRollupAction
import com.colasoft.opensearch.indexmanagement.rollup.action.explain.ExplainRollupAction
import com.colasoft.opensearch.indexmanagement.rollup.action.explain.TransportExplainRollupAction
import com.colasoft.opensearch.indexmanagement.rollup.action.get.GetRollupAction
import com.colasoft.opensearch.indexmanagement.rollup.action.get.GetRollupsAction
import com.colasoft.opensearch.indexmanagement.rollup.action.get.TransportGetRollupAction
import com.colasoft.opensearch.indexmanagement.rollup.action.get.TransportGetRollupsAction
import com.colasoft.opensearch.indexmanagement.rollup.action.index.IndexRollupAction
import com.colasoft.opensearch.indexmanagement.rollup.action.index.TransportIndexRollupAction
import com.colasoft.opensearch.indexmanagement.rollup.action.mapping.TransportUpdateRollupMappingAction
import com.colasoft.opensearch.indexmanagement.rollup.action.mapping.UpdateRollupMappingAction
import com.colasoft.opensearch.indexmanagement.rollup.action.start.StartRollupAction
import com.colasoft.opensearch.indexmanagement.rollup.action.start.TransportStartRollupAction
import com.colasoft.opensearch.indexmanagement.rollup.action.stop.StopRollupAction
import com.colasoft.opensearch.indexmanagement.rollup.action.stop.TransportStopRollupAction
import com.colasoft.opensearch.indexmanagement.rollup.actionfilter.FieldCapsFilter
import com.colasoft.opensearch.indexmanagement.rollup.interceptor.RollupInterceptor
import com.colasoft.opensearch.indexmanagement.rollup.model.Rollup
import com.colasoft.opensearch.indexmanagement.rollup.model.RollupMetadata
import com.colasoft.opensearch.indexmanagement.rollup.resthandler.RestDeleteRollupAction
import com.colasoft.opensearch.indexmanagement.rollup.resthandler.RestExplainRollupAction
import com.colasoft.opensearch.indexmanagement.rollup.resthandler.RestGetRollupAction
import com.colasoft.opensearch.indexmanagement.rollup.resthandler.RestIndexRollupAction
import com.colasoft.opensearch.indexmanagement.rollup.resthandler.RestStartRollupAction
import com.colasoft.opensearch.indexmanagement.rollup.resthandler.RestStopRollupAction
import com.colasoft.opensearch.indexmanagement.rollup.settings.LegacyOpenDistroRollupSettings
import com.colasoft.opensearch.indexmanagement.rollup.settings.RollupSettings
import com.colasoft.opensearch.indexmanagement.rollup.util.QueryShardContextFactory
import com.colasoft.opensearch.indexmanagement.rollup.util.RollupFieldValueExpressionResolver
import com.colasoft.opensearch.indexmanagement.settings.IndexManagementSettings
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.resthandler.RestCreateSMPolicyHandler
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.resthandler.RestDeleteSMPolicyHandler
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.resthandler.RestExplainSMPolicyHandler
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.resthandler.RestGetSMPolicyHandler
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.resthandler.RestStartSMPolicyHandler
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.resthandler.RestStopSMPolicyHandler
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.resthandler.RestUpdateSMPolicyHandler
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.SMActions
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.delete.TransportDeleteSMPolicyAction
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.explain.TransportExplainSMAction
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.get.TransportGetSMPoliciesAction
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.get.TransportGetSMPolicyAction
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.index.TransportIndexSMPolicyAction
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.start.TransportStartSMAction
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.api.transport.stop.TransportStopSMAction
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.SMRunner
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.model.SMMetadata
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.model.SMPolicy
import com.colasoft.opensearch.indexmanagement.snapshotmanagement.settings.SnapshotManagementSettings
import com.colasoft.opensearch.indexmanagement.spi.IndexManagementExtension
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.IndexMetadataService
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.StatusChecker
import com.colasoft.opensearch.indexmanagement.spi.indexstatemanagement.model.ManagedIndexMetaData
import com.colasoft.opensearch.indexmanagement.transform.TransformRunner
import com.colasoft.opensearch.indexmanagement.transform.action.delete.DeleteTransformsAction
import com.colasoft.opensearch.indexmanagement.transform.action.delete.TransportDeleteTransformsAction
import com.colasoft.opensearch.indexmanagement.transform.action.explain.ExplainTransformAction
import com.colasoft.opensearch.indexmanagement.transform.action.explain.TransportExplainTransformAction
import com.colasoft.opensearch.indexmanagement.transform.action.get.GetTransformAction
import com.colasoft.opensearch.indexmanagement.transform.action.get.GetTransformsAction
import com.colasoft.opensearch.indexmanagement.transform.action.get.TransportGetTransformAction
import com.colasoft.opensearch.indexmanagement.transform.action.get.TransportGetTransformsAction
import com.colasoft.opensearch.indexmanagement.transform.action.index.IndexTransformAction
import com.colasoft.opensearch.indexmanagement.transform.action.index.TransportIndexTransformAction
import com.colasoft.opensearch.indexmanagement.transform.action.preview.PreviewTransformAction
import com.colasoft.opensearch.indexmanagement.transform.action.preview.TransportPreviewTransformAction
import com.colasoft.opensearch.indexmanagement.transform.action.start.StartTransformAction
import com.colasoft.opensearch.indexmanagement.transform.action.start.TransportStartTransformAction
import com.colasoft.opensearch.indexmanagement.transform.action.stop.StopTransformAction
import com.colasoft.opensearch.indexmanagement.transform.action.stop.TransportStopTransformAction
import com.colasoft.opensearch.indexmanagement.transform.model.Transform
import com.colasoft.opensearch.indexmanagement.transform.model.TransformMetadata
import com.colasoft.opensearch.indexmanagement.transform.resthandler.RestDeleteTransformAction
import com.colasoft.opensearch.indexmanagement.transform.resthandler.RestExplainTransformAction
import com.colasoft.opensearch.indexmanagement.transform.resthandler.RestGetTransformAction
import com.colasoft.opensearch.indexmanagement.transform.resthandler.RestIndexTransformAction
import com.colasoft.opensearch.indexmanagement.transform.resthandler.RestPreviewTransformAction
import com.colasoft.opensearch.indexmanagement.transform.resthandler.RestStartTransformAction
import com.colasoft.opensearch.indexmanagement.transform.resthandler.RestStopTransformAction
import com.colasoft.opensearch.indexmanagement.transform.settings.TransformSettings
import com.colasoft.opensearch.jobscheduler.spi.JobSchedulerExtension
import com.colasoft.opensearch.jobscheduler.spi.ScheduledJobParser
import com.colasoft.opensearch.jobscheduler.spi.ScheduledJobRunner
import com.colasoft.opensearch.monitor.jvm.JvmService
import com.colasoft.opensearch.plugins.ActionPlugin
import com.colasoft.opensearch.plugins.ExtensiblePlugin
import com.colasoft.opensearch.plugins.NetworkPlugin
import com.colasoft.opensearch.plugins.Plugin
import com.colasoft.opensearch.repositories.RepositoriesService
import com.colasoft.opensearch.rest.RestController
import com.colasoft.opensearch.rest.RestHandler
import com.colasoft.opensearch.script.ScriptService
import com.colasoft.opensearch.threadpool.ThreadPool
import com.colasoft.opensearch.transport.RemoteClusterService
import com.colasoft.opensearch.transport.TransportInterceptor
import com.colasoft.opensearch.transport.TransportService
import com.colasoft.opensearch.watcher.ResourceWatcherService
import java.util.function.Supplier

@Suppress("TooManyFunctions")
class IndexManagementPlugin : JobSchedulerExtension, NetworkPlugin, ActionPlugin, ExtensiblePlugin, Plugin() {

    private val logger = LogManager.getLogger(javaClass)
    lateinit var indexManagementIndices: IndexManagementIndices
    lateinit var actionValidation: ActionValidation
    lateinit var clusterService: ClusterService
    lateinit var indexNameExpressionResolver: IndexNameExpressionResolver
    lateinit var rollupInterceptor: RollupInterceptor
    lateinit var fieldCapsFilter: FieldCapsFilter
    lateinit var indexMetadataProvider: IndexMetadataProvider
    private val indexMetadataServices: MutableList<Map<String, IndexMetadataService>> = mutableListOf()
    private var customIndexUUIDSetting: String? = null
    private val extensions = mutableSetOf<String>()
    private val extensionCheckerMap = mutableMapOf<String, StatusChecker>()

    companion object {
        const val PLUGINS_BASE_URI = "/_plugins"
        const val ISM_BASE_URI = "$PLUGINS_BASE_URI/_ism"
        const val ROLLUP_BASE_URI = "$PLUGINS_BASE_URI/_rollup"
        const val TRANSFORM_BASE_URI = "$PLUGINS_BASE_URI/_transform"
        const val POLICY_BASE_URI = "$ISM_BASE_URI/policies"
        const val ROLLUP_JOBS_BASE_URI = "$ROLLUP_BASE_URI/jobs"
        const val INDEX_MANAGEMENT_INDEX = ".opendistro-ism-config"
        const val INDEX_MANAGEMENT_JOB_TYPE = "opendistro-index-management"
        const val INDEX_STATE_MANAGEMENT_HISTORY_TYPE = "managed_index_meta_data"

        const val SM_BASE_URI = "$PLUGINS_BASE_URI/_sm"
        const val SM_POLICIES_URI = "$SM_BASE_URI/policies"

        const val OLD_PLUGIN_NAME = "opendistro-im"
        const val OPEN_DISTRO_BASE_URI = "/_opendistro"
        const val LEGACY_ISM_BASE_URI = "$OPEN_DISTRO_BASE_URI/_ism"
        const val LEGACY_ROLLUP_BASE_URI = "$OPEN_DISTRO_BASE_URI/_rollup"
        const val LEGACY_POLICY_BASE_URI = "$LEGACY_ISM_BASE_URI/policies"
        const val LEGACY_ROLLUP_JOBS_BASE_URI = "$LEGACY_ROLLUP_BASE_URI/jobs"
    }

    override fun getJobIndex(): String = INDEX_MANAGEMENT_INDEX

    override fun getJobType(): String = INDEX_MANAGEMENT_JOB_TYPE

    override fun getJobRunner(): ScheduledJobRunner = IndexManagementRunner

    override fun getGuiceServiceClasses(): Collection<Class<out LifecycleComponent?>> {
        return mutableListOf<Class<out LifecycleComponent?>>(GuiceHolder::class.java)
    }

    @Suppress("ComplexMethod")
    override fun getJobParser(): ScheduledJobParser {
        return ScheduledJobParser { xcp, id, jobDocVersion ->
            ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp)
            while (xcp.nextToken() != Token.END_OBJECT) {
                val fieldName = xcp.currentName()
                xcp.nextToken()

                when (fieldName) {
                    ManagedIndexConfig.MANAGED_INDEX_TYPE -> {
                        return@ScheduledJobParser ManagedIndexConfig.parse(xcp, id, jobDocVersion.seqNo, jobDocVersion.primaryTerm)
                    }
                    Policy.POLICY_TYPE -> {
                        return@ScheduledJobParser null
                    }
                    Rollup.ROLLUP_TYPE -> {
                        return@ScheduledJobParser Rollup.parse(xcp, id, jobDocVersion.seqNo, jobDocVersion.primaryTerm)
                    }
                    RollupMetadata.ROLLUP_METADATA_TYPE -> {
                        return@ScheduledJobParser null
                    }
                    Transform.TRANSFORM_TYPE -> {
                        return@ScheduledJobParser Transform.parse(xcp, id, jobDocVersion.seqNo, jobDocVersion.primaryTerm)
                    }
                    TransformMetadata.TRANSFORM_METADATA_TYPE -> {
                        return@ScheduledJobParser null
                    }
                    ManagedIndexMetaData.MANAGED_INDEX_METADATA_TYPE -> {
                        return@ScheduledJobParser null
                    }
                    SMPolicy.SM_TYPE -> {
                        return@ScheduledJobParser SMPolicy.parse(xcp, id, jobDocVersion.seqNo, jobDocVersion.primaryTerm)
                    }
                    SMMetadata.SM_METADATA_TYPE -> {
                        return@ScheduledJobParser null
                    }
                    else -> {
                        logger.warn("Unsupported document was indexed in $INDEX_MANAGEMENT_INDEX with type: $fieldName")
                        xcp.skipChildren()
                    }
                }
            }
            return@ScheduledJobParser null
        }
    }

    override fun loadExtensions(loader: ExtensiblePlugin.ExtensionLoader) {
        val indexManagementExtensions = loader.loadExtensions(IndexManagementExtension::class.java)

        indexManagementExtensions.forEach { extension ->
            val extensionName = extension.getExtensionName()
            if (extensionName in extensions) {
                error("Multiple extensions of IndexManagement have same name $extensionName - not supported")
            }
            extension.getISMActionParsers().forEach { parser ->
                ISMActionsParser.instance.addParser(parser, extensionName)
            }
            indexMetadataServices.add(extension.getIndexMetadataService())
            extension.overrideClusterStateIndexUuidSetting()?.let {
                if (customIndexUUIDSetting != null) {
                    error(
                        "Multiple extensions of IndexManagement plugin overriding ClusterStateIndexUUIDSetting - not supported"
                    )
                }
                customIndexUUIDSetting = extension.overrideClusterStateIndexUuidSetting()
            }
            extensionCheckerMap[extensionName] = extension.statusChecker()
        }
    }

    override fun getRestHandlers(
        settings: Settings,
        restController: RestController,
        clusterSettings: ClusterSettings,
        indexScopedSettings: IndexScopedSettings,
        settingsFilter: SettingsFilter,
        indexNameExpressionResolver: IndexNameExpressionResolver,
        nodesInCluster: Supplier<DiscoveryNodes>
    ): List<RestHandler> {
        return listOf(
            RestRefreshSearchAnalyzerAction(),
            RestIndexPolicyAction(settings, clusterService),
            RestGetPolicyAction(),
            RestDeletePolicyAction(),
            RestExplainAction(),
            RestRetryFailedManagedIndexAction(),
            RestAddPolicyAction(),
            RestRemovePolicyAction(),
            RestChangePolicyAction(),
            RestDeleteRollupAction(),
            RestGetRollupAction(),
            RestIndexRollupAction(),
            RestStartRollupAction(),
            RestStopRollupAction(),
            RestExplainRollupAction(),
            RestIndexTransformAction(),
            RestGetTransformAction(),
            RestPreviewTransformAction(),
            RestDeleteTransformAction(),
            RestExplainTransformAction(),
            RestStartTransformAction(),
            RestStopTransformAction(),
            RestGetSMPolicyHandler(),
            RestStartSMPolicyHandler(),
            RestStopSMPolicyHandler(),
            RestExplainSMPolicyHandler(),
            RestDeleteSMPolicyHandler(),
            RestCreateSMPolicyHandler(),
            RestUpdateSMPolicyHandler()
        )
    }

    @Suppress("LongMethod")
    override fun createComponents(
        client: Client,
        clusterService: ClusterService,
        threadPool: ThreadPool,
        resourceWatcherService: ResourceWatcherService,
        scriptService: ScriptService,
        xContentRegistry: NamedXContentRegistry,
        environment: Environment,
        nodeEnvironment: NodeEnvironment,
        namedWriteableRegistry: NamedWriteableRegistry,
        indexNameExpressionResolver: IndexNameExpressionResolver,
        repositoriesServiceSupplier: Supplier<RepositoriesService>
    ): Collection<Any> {
        val settings = environment.settings()
        this.clusterService = clusterService
        QueryShardContextFactory.init(
            client,
            clusterService,
            scriptService,
            xContentRegistry,
            namedWriteableRegistry,
            environment
        )
        rollupInterceptor = RollupInterceptor(clusterService, settings, indexNameExpressionResolver)
        val jvmService = JvmService(environment.settings())
        val transformRunner = TransformRunner.initialize(
            client,
            clusterService,
            xContentRegistry,
            settings,
            indexNameExpressionResolver,
            jvmService,
            threadPool
        )
        fieldCapsFilter = FieldCapsFilter(clusterService, settings, indexNameExpressionResolver)
        this.indexNameExpressionResolver = indexNameExpressionResolver

        val skipFlag = SkipExecution(client)
        RollupFieldValueExpressionResolver.registerServices(scriptService, clusterService)
        val rollupRunner = RollupRunner
            .registerClient(client)
            .registerClusterService(clusterService)
            .registerNamedXContentRegistry(xContentRegistry)
            .registerScriptService(scriptService)
            .registerSettings(settings)
            .registerThreadPool(threadPool)
            .registerMapperService(RollupMapperService(client, clusterService, indexNameExpressionResolver))
            .registerIndexer(RollupIndexer(settings, clusterService, client))
            .registerSearcher(RollupSearchService(settings, clusterService, client))
            .registerMetadataServices(RollupMetadataService(client, xContentRegistry))
            .registerConsumers()
            .registerClusterConfigurationProvider(skipFlag)
        indexManagementIndices = IndexManagementIndices(settings, client.admin().indices(), clusterService)
        actionValidation = ActionValidation(settings, clusterService, jvmService)
        val indexStateManagementHistory =
            IndexStateManagementHistory(
                settings,
                client,
                threadPool,
                clusterService,
                indexManagementIndices
            )

        indexMetadataProvider = IndexMetadataProvider(
            settings, client, clusterService,
            hashMapOf(
                DEFAULT_INDEX_TYPE to DefaultIndexMetadataService(customIndexUUIDSetting)
            )
        )
        indexMetadataServices.forEach { indexMetadataProvider.addMetadataServices(it) }

        val extensionChecker = ExtensionStatusChecker(extensionCheckerMap, clusterService)
        val managedIndexRunner = ManagedIndexRunner
            .registerClient(client)
            .registerClusterService(clusterService)
            .registerValidationService(actionValidation)
            .registerNamedXContentRegistry(xContentRegistry)
            .registerScriptService(scriptService)
            .registerSettings(settings)
            .registerConsumers() // registerConsumers must happen after registerSettings/clusterService
            .registerIMIndex(indexManagementIndices)
            .registerHistoryIndex(indexStateManagementHistory)
            .registerSkipFlag(skipFlag)
            .registerThreadPool(threadPool)
            .registerExtensionChecker(extensionChecker)
            .registerIndexMetadataProvider(indexMetadataProvider)

        val metadataService = MetadataService(client, clusterService, skipFlag, indexManagementIndices)
        val templateService = ISMTemplateService(client, clusterService, xContentRegistry, indexManagementIndices)

        val managedIndexCoordinator = ManagedIndexCoordinator(
            environment.settings(),
            client, clusterService, threadPool, indexManagementIndices, metadataService, templateService, indexMetadataProvider
        )

        val smRunner = SMRunner.init(client, threadPool, settings, indexManagementIndices, clusterService)

        val pluginVersionSweepCoordinator = PluginVersionSweepCoordinator(skipFlag, settings, threadPool, clusterService)

        return listOf(
            managedIndexRunner,
            rollupRunner,
            transformRunner,
            indexManagementIndices,
            actionValidation,
            managedIndexCoordinator,
            indexStateManagementHistory,
            indexMetadataProvider,
            smRunner,
            pluginVersionSweepCoordinator
        )
    }

    @Suppress("LongMethod")
    override fun getSettings(): List<Setting<*>> {
        return listOf(
            ManagedIndexSettings.HISTORY_ENABLED,
            ManagedIndexSettings.HISTORY_INDEX_MAX_AGE,
            ManagedIndexSettings.HISTORY_MAX_DOCS,
            ManagedIndexSettings.HISTORY_RETENTION_PERIOD,
            ManagedIndexSettings.HISTORY_ROLLOVER_CHECK_PERIOD,
            ManagedIndexSettings.HISTORY_NUMBER_OF_SHARDS,
            ManagedIndexSettings.HISTORY_NUMBER_OF_REPLICAS,
            ManagedIndexSettings.POLICY_ID,
            ManagedIndexSettings.ROLLOVER_ALIAS,
            ManagedIndexSettings.ROLLOVER_SKIP,
            ManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED,
            ManagedIndexSettings.ACTION_VALIDATION_ENABLED,
            ManagedIndexSettings.METADATA_SERVICE_ENABLED,
            ManagedIndexSettings.AUTO_MANAGE,
            ManagedIndexSettings.METADATA_SERVICE_STATUS,
            ManagedIndexSettings.TEMPLATE_MIGRATION_CONTROL,
            ManagedIndexSettings.JITTER,
            ManagedIndexSettings.JOB_INTERVAL,
            ManagedIndexSettings.SWEEP_PERIOD,
            ManagedIndexSettings.SWEEP_SKIP_PERIOD,
            ManagedIndexSettings.COORDINATOR_BACKOFF_COUNT,
            ManagedIndexSettings.COORDINATOR_BACKOFF_MILLIS,
            ManagedIndexSettings.ALLOW_LIST,
            ManagedIndexSettings.SNAPSHOT_DENY_LIST,
            ManagedIndexSettings.RESTRICTED_INDEX_PATTERN,
            RollupSettings.ROLLUP_INGEST_BACKOFF_COUNT,
            RollupSettings.ROLLUP_INGEST_BACKOFF_MILLIS,
            RollupSettings.ROLLUP_SEARCH_BACKOFF_COUNT,
            RollupSettings.ROLLUP_SEARCH_BACKOFF_MILLIS,
            RollupSettings.ROLLUP_INDEX,
            RollupSettings.ROLLUP_ENABLED,
            RollupSettings.ROLLUP_SEARCH_ENABLED,
            RollupSettings.ROLLUP_DASHBOARDS,
            RollupSettings.ROLLUP_SEARCH_ALL_JOBS,
            TransformSettings.TRANSFORM_JOB_INDEX_BACKOFF_COUNT,
            TransformSettings.TRANSFORM_JOB_INDEX_BACKOFF_MILLIS,
            TransformSettings.TRANSFORM_JOB_SEARCH_BACKOFF_COUNT,
            TransformSettings.TRANSFORM_JOB_SEARCH_BACKOFF_MILLIS,
            TransformSettings.TRANSFORM_CIRCUIT_BREAKER_ENABLED,
            TransformSettings.TRANSFORM_CIRCUIT_BREAKER_JVM_THRESHOLD,
            IndexManagementSettings.FILTER_BY_BACKEND_ROLES,
            LegacyOpenDistroManagedIndexSettings.HISTORY_ENABLED,
            LegacyOpenDistroManagedIndexSettings.HISTORY_INDEX_MAX_AGE,
            LegacyOpenDistroManagedIndexSettings.HISTORY_MAX_DOCS,
            LegacyOpenDistroManagedIndexSettings.HISTORY_RETENTION_PERIOD,
            LegacyOpenDistroManagedIndexSettings.HISTORY_ROLLOVER_CHECK_PERIOD,
            LegacyOpenDistroManagedIndexSettings.HISTORY_NUMBER_OF_SHARDS,
            LegacyOpenDistroManagedIndexSettings.HISTORY_NUMBER_OF_REPLICAS,
            LegacyOpenDistroManagedIndexSettings.POLICY_ID,
            LegacyOpenDistroManagedIndexSettings.ROLLOVER_ALIAS,
            LegacyOpenDistroManagedIndexSettings.ROLLOVER_SKIP,
            LegacyOpenDistroManagedIndexSettings.INDEX_STATE_MANAGEMENT_ENABLED,
            LegacyOpenDistroManagedIndexSettings.METADATA_SERVICE_ENABLED,
            LegacyOpenDistroManagedIndexSettings.JOB_INTERVAL,
            LegacyOpenDistroManagedIndexSettings.SWEEP_PERIOD,
            LegacyOpenDistroManagedIndexSettings.COORDINATOR_BACKOFF_COUNT,
            LegacyOpenDistroManagedIndexSettings.COORDINATOR_BACKOFF_MILLIS,
            LegacyOpenDistroManagedIndexSettings.ALLOW_LIST,
            LegacyOpenDistroManagedIndexSettings.SNAPSHOT_DENY_LIST,
            LegacyOpenDistroManagedIndexSettings.AUTO_MANAGE,
            LegacyOpenDistroManagedIndexSettings.METADATA_SERVICE_STATUS,
            LegacyOpenDistroManagedIndexSettings.TEMPLATE_MIGRATION_CONTROL,
            LegacyOpenDistroManagedIndexSettings.RESTRICTED_INDEX_PATTERN,
            LegacyOpenDistroRollupSettings.ROLLUP_INGEST_BACKOFF_COUNT,
            LegacyOpenDistroRollupSettings.ROLLUP_INGEST_BACKOFF_MILLIS,
            LegacyOpenDistroRollupSettings.ROLLUP_SEARCH_BACKOFF_COUNT,
            LegacyOpenDistroRollupSettings.ROLLUP_SEARCH_BACKOFF_MILLIS,
            LegacyOpenDistroRollupSettings.ROLLUP_INDEX,
            LegacyOpenDistroRollupSettings.ROLLUP_ENABLED,
            LegacyOpenDistroRollupSettings.ROLLUP_SEARCH_ENABLED,
            LegacyOpenDistroRollupSettings.ROLLUP_DASHBOARDS,
            SnapshotManagementSettings.FILTER_BY_BACKEND_ROLES
        )
    }

    override fun getActions(): List<ActionPlugin.ActionHandler<out ActionRequest, out ActionResponse>> {
        return listOf(
            ActionPlugin.ActionHandler(UpdateManagedIndexMetaDataAction.INSTANCE, TransportUpdateManagedIndexMetaDataAction::class.java),
            ActionPlugin.ActionHandler(RemovePolicyAction.INSTANCE, TransportRemovePolicyAction::class.java),
            ActionPlugin.ActionHandler(RefreshSearchAnalyzerAction.INSTANCE, TransportRefreshSearchAnalyzerAction::class.java),
            ActionPlugin.ActionHandler(AddPolicyAction.INSTANCE, TransportAddPolicyAction::class.java),
            ActionPlugin.ActionHandler(RetryFailedManagedIndexAction.INSTANCE, TransportRetryFailedManagedIndexAction::class.java),
            ActionPlugin.ActionHandler(ChangePolicyAction.INSTANCE, TransportChangePolicyAction::class.java),
            ActionPlugin.ActionHandler(IndexPolicyAction.INSTANCE, TransportIndexPolicyAction::class.java),
            ActionPlugin.ActionHandler(ExplainAction.INSTANCE, TransportExplainAction::class.java),
            ActionPlugin.ActionHandler(DeletePolicyAction.INSTANCE, TransportDeletePolicyAction::class.java),
            ActionPlugin.ActionHandler(GetPolicyAction.INSTANCE, TransportGetPolicyAction::class.java),
            ActionPlugin.ActionHandler(GetPoliciesAction.INSTANCE, TransportGetPoliciesAction::class.java),
            ActionPlugin.ActionHandler(DeleteRollupAction.INSTANCE, TransportDeleteRollupAction::class.java),
            ActionPlugin.ActionHandler(GetRollupAction.INSTANCE, TransportGetRollupAction::class.java),
            ActionPlugin.ActionHandler(GetRollupsAction.INSTANCE, TransportGetRollupsAction::class.java),
            ActionPlugin.ActionHandler(IndexRollupAction.INSTANCE, TransportIndexRollupAction::class.java),
            ActionPlugin.ActionHandler(StartRollupAction.INSTANCE, TransportStartRollupAction::class.java),
            ActionPlugin.ActionHandler(StopRollupAction.INSTANCE, TransportStopRollupAction::class.java),
            ActionPlugin.ActionHandler(ExplainRollupAction.INSTANCE, TransportExplainRollupAction::class.java),
            ActionPlugin.ActionHandler(UpdateRollupMappingAction.INSTANCE, TransportUpdateRollupMappingAction::class.java),
            ActionPlugin.ActionHandler(IndexTransformAction.INSTANCE, TransportIndexTransformAction::class.java),
            ActionPlugin.ActionHandler(GetTransformAction.INSTANCE, TransportGetTransformAction::class.java),
            ActionPlugin.ActionHandler(GetTransformsAction.INSTANCE, TransportGetTransformsAction::class.java),
            ActionPlugin.ActionHandler(PreviewTransformAction.INSTANCE, TransportPreviewTransformAction::class.java),
            ActionPlugin.ActionHandler(DeleteTransformsAction.INSTANCE, TransportDeleteTransformsAction::class.java),
            ActionPlugin.ActionHandler(ExplainTransformAction.INSTANCE, TransportExplainTransformAction::class.java),
            ActionPlugin.ActionHandler(StartTransformAction.INSTANCE, TransportStartTransformAction::class.java),
            ActionPlugin.ActionHandler(StopTransformAction.INSTANCE, TransportStopTransformAction::class.java),
            ActionPlugin.ActionHandler(ManagedIndexAction.INSTANCE, TransportManagedIndexAction::class.java),
            ActionPlugin.ActionHandler(SMActions.INDEX_SM_POLICY_ACTION_TYPE, TransportIndexSMPolicyAction::class.java),
            ActionPlugin.ActionHandler(SMActions.GET_SM_POLICY_ACTION_TYPE, TransportGetSMPolicyAction::class.java),
            ActionPlugin.ActionHandler(SMActions.DELETE_SM_POLICY_ACTION_TYPE, TransportDeleteSMPolicyAction::class.java),
            ActionPlugin.ActionHandler(SMActions.START_SM_POLICY_ACTION_TYPE, TransportStartSMAction::class.java),
            ActionPlugin.ActionHandler(SMActions.STOP_SM_POLICY_ACTION_TYPE, TransportStopSMAction::class.java),
            ActionPlugin.ActionHandler(SMActions.EXPLAIN_SM_POLICY_ACTION_TYPE, TransportExplainSMAction::class.java),
            ActionPlugin.ActionHandler(SMActions.GET_SM_POLICIES_ACTION_TYPE, TransportGetSMPoliciesAction::class.java)
        )
    }

    override fun getTransportInterceptors(namedWriteableRegistry: NamedWriteableRegistry, threadContext: ThreadContext): List<TransportInterceptor> {
        return listOf(rollupInterceptor)
    }

    override fun getActionFilters(): List<ActionFilter> {
        return listOf(fieldCapsFilter)
    }
}

class GuiceHolder @Inject constructor(
    remoteClusterService: TransportService
) : LifecycleComponent {
    override fun close() { /* do nothing */ }
    override fun lifecycleState(): Lifecycle.State? {
        return null
    }

    override fun addLifecycleListener(listener: LifecycleListener) { /* do nothing */ }
    override fun removeLifecycleListener(listener: LifecycleListener) { /* do nothing */ }
    override fun start() { /* do nothing */ }
    override fun stop() { /* do nothing */ }

    companion object {
        lateinit var remoteClusterService: RemoteClusterService
    }

    init {
        Companion.remoteClusterService = remoteClusterService.remoteClusterService
    }
}
