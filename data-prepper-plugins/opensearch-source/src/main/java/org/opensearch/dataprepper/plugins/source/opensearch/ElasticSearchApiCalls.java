/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *//*


package org.opensearch.dataprepper.plugins.source.opensearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.core.ScrollRequest;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.stream.Collectors;

public class ElasticSearchApiCalls implements SearchAPICalls {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchApiCalls.class);
    private static final String KEEP_ALIVE_VALUE = "24h";
    private static final String TIME_VALUE = "24h";
    private static final int ELASTIC_SEARCH_VERSION = 7100;


    @Override
    public String generatePitId(final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        OpenPointInTimeResponse response = null;
        OpenPointInTimeRequest request = new OpenPointInTimeRequest.Builder().
                index(openSearchSourceConfiguration.getIndexValue()).
                keepAlive(new Time.Builder().time(KEEP_ALIVE_VALUE).build()).build();
            LOG.info("Request is : {} ", request);
            try {
                response = client.openPointInTime(request);
                LOG.debug("Response is {} ",response);
            } catch (Exception ex) {
                LOG.error(" {}",ex.getMessage());
            }

        return response.id();
    }
    @Override
    public String searchPitIndexes(final String pitId,final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        SearchResponse<ObjectNode> searchResponse = null;
        try {
            searchResponse = client.search(req ->
                            req.index(openSearchSourceConfiguration.getIndexValue()),
                    ObjectNode.class);
            searchResponse.hits().hits().stream()
                    .map(Hit::source).collect(Collectors.toList());
            LOG.debug("Search Response {} ", searchResponse);

        } catch (Exception ex) {
            LOG.error(ex.getMessage());
        }
        return searchResponse.toString();
    }
    @Override
    public String generateScrollId(final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        SearchResponse response = null;
        StringBuilder indexList = Utility.getIndexList(openSearchSourceConfiguration);
        SearchRequest searchRequest = SearchRequest
                .of(e -> e.index(indexList.toString()).size(openSearchSourceConfiguration.getSearchConfiguration().getBatchSize()).scroll(scr -> scr.time(TIME_VALUE)));
        try {
            response = client.search(searchRequest, ObjectNode.class);
            LOG.info("Response is : {} ",response);
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        return response.scrollId();
    }
    public co.elastic.clients.elasticsearch.core.ScrollRequest nextScrollRequest(final String scrollId) {
        return ScrollRequest
                .of(scrollRequest -> scrollRequest.scrollId(scrollId).scroll(Time.of(t -> t.time(TIME_VALUE))));
    }
    @Override
    public String searchScrollIndexes(final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        return null;
    }


    @Override
    public Boolean delete(final String id,final Integer elasticSearchVersion) {
        LOG.info("PIT or Scroll ID to be deleted - " + id);
        try {
            if (elasticSearchVersion.intValue() >= ELASTICSEARCH_VERSION) {
                return deletePitId(id, client);
            } else {
                return deleteScrollId(id, client);

            }

        } catch (IOException e) {
            LOG.error("Error occured while closing PIT " + e);
        }
        return false;
    }

    private boolean deleteScrollId(String id) throws IOException {
        ClearScrollRequest scrollRequest=new ClearScrollRequest.Builder().scrollId(id).build();
        ClearScrollResponse clearScrollResponse = client.clearScroll(scrollRequest);
        LOG.info("Delete Scroll ID Response "+clearScrollResponse);
        return  clearScrollResponse.succeeded();
    }

    private boolean deletePitId(String id, ElasticsearchClient client) throws IOException {
        ClosePointInTimeRequest request = new ClosePointInTimeRequest.Builder().id(id).build();
        ClosePointInTimeResponse closePointInTimeResponse = client.closePointInTime(request);
        LOG.info("Delete PIT ID Response " + closePointInTimeResponse);
        return closePointInTimeResponse.succeeded();
    }
}*/
