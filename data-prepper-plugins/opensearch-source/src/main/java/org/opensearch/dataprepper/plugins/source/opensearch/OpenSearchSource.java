/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.json.simple.parser.ParseException;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.client.opensearch.cat.indices.IndicesRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

@DataPrepperPlugin(name="opensearch", pluginType = Source.class , pluginConfigurationType =OpenSearchSourceConfiguration.class )
public class OpenSearchSource implements Source<Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchSource.class);

    private static final String ELASTIC_SEARCH = "elasticsearch";

    private static final String OPEN_SEARCH = "opensearch";

    private OpenSearchClient osClient;

    private ElasticsearchClient esClient;

    private final OpenSearchSourceConfiguration openSearchSourceConfiguration;

    @DataPrepperPluginConstructor
    public OpenSearchSource(final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        this.openSearchSourceConfiguration = openSearchSourceConfiguration;
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }
        startProcess(openSearchSourceConfiguration,buffer);
    }

    private void startProcess(final OpenSearchSourceConfiguration openSearchSourceConfiguration,Buffer<Record<Event>> buffer) {
        PrepareConnection prepareConnection = new PrepareConnection();

        try {
            SourceInfo sourceInfo = new SourceInfo();
            SourceInfoProvider sourceInfoProvider = new SourceInfoProvider();
            String datasource = sourceInfoProvider.getSourceInfo(openSearchSourceConfiguration);
            sourceInfo.setDataSource(datasource);
            LOG.info("Datasource is : {} ", sourceInfo.getDataSource());
            sourceInfo = sourceInfoProvider.checkStatus(openSearchSourceConfiguration, sourceInfo);
            if (Boolean.TRUE.equals(sourceInfo.getHealthStatus())) {
                if (OPEN_SEARCH.equalsIgnoreCase(datasource)) {
                    osClient = prepareConnection.prepareOpensearchConnection(openSearchSourceConfiguration);
                    sourceInfoProvider.getCatOpenSearchIndices(openSearchSourceConfiguration, osClient);
                    sourceInfoProvider.versionCheckForOpenSearch(openSearchSourceConfiguration, sourceInfo, osClient,buffer);
                } else {
                    esClient = prepareConnection.prepareElasticSearchConnection();
                    sourceInfoProvider.getCatElasticIndices(openSearchSourceConfiguration,esClient);
                    sourceInfoProvider.versionCheckForElasticSearch(openSearchSourceConfiguration, sourceInfo, esClient,buffer);
                }

            } else {
               // retry cluster health not good
            }
        } catch ( Exception e ){

        }

           /* LOG.info("Block to re-check the cluster health after sometime");
                BackoffService backoff = new BackoffService(openSearchSourceConfiguration.getMaxRetry());
                backoff.waitUntilNextTry();
                while (backoff.shouldRetry()) {
                    sourceInfo = sourceInfoProvider.checkStatus(openSearchSourceConfiguration,sourceInfo);
                    if (Boolean.TRUE.equals(sourceInfo.getHealthStatus())) {
                        backoff.doNotRetry();
                        callVersionApi();
                        break;
                    } else {
                        LOG.info("** Retrying after sometime **");
                        backoff.errorOccured();
                    }
                } */


    }

    @Override
    public void stop() {

    }

   /* private SourceInfoProvider callVersionApi() {
        PrepareConnection prepareConnection = new PrepareConnection();
        client = prepareConnection.restHighprepareOpensearchConnection();
        sourceInfoProvider.versionCheck(openSearchSourceConfiguration, sourceInfo, client);
        return sourceInfoProvider;
    }*/
}
