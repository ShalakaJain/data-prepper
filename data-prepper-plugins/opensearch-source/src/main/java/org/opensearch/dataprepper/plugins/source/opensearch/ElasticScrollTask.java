/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.opensearch.ElasticSearchApiCalls;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchApiCalls;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchSourceConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.TimerTask;
import java.util.concurrent.TimeoutException;

public class ElasticScrollTask extends TimerTask {
    
    private static final Logger LOG = LoggerFactory.getLogger(ElasticScrollTask.class);
    
    private OpenSearchSourceConfiguration openSearchSourceConfiguration = null;

    private ElasticsearchClient esClient = null;

    private Buffer<Record<Event>> buffer = null;

    private ElasticSearchApiCalls elasticSearchApiCalls = null;

    public ElasticScrollTask(OpenSearchSourceConfiguration openSearchSourceConfiguration , Buffer<Record<Event>> buffer , ElasticsearchClient esClient ) {
        this.openSearchSourceConfiguration = openSearchSourceConfiguration;
        this.buffer = buffer;
        elasticSearchApiCalls = new ElasticSearchApiCalls(esClient);
    }

    @Override
    public void run() {
        int numRuns = 0;
        while (numRuns++ <= openSearchSourceConfiguration.getSchedulingParameterConfiguration().getJobCount()) {
                elasticSearchApiCalls.getScrollResponse(openSearchSourceConfiguration,buffer);
        }
    }
}