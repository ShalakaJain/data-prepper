/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.core.ScrollRequest;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hc.core5.http.HttpStatus;
import org.json.simple.JSONObject;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SortingConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class ElasticSearchApiCalls implements SearchAPICalls {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchApiCalls.class);

    private static final String KEEP_ALIVE_VALUE = "24h";

    private static final String TIME_VALUE = "24h";

    private static final int ELASTIC_SEARCH_VERSION = 7100;

    private static final int SEARCH_AFTER_SIZE = 100;

    private ElasticsearchClient elasticsearchClient;

    private SourceInfoProvider sourceInfoProvider = new SourceInfoProvider();

    public ElasticSearchApiCalls(ElasticsearchClient elasticsearchClient) {
        this.elasticsearchClient = elasticsearchClient;
    }

    @Override
    public void generatePitId(final OpenSearchSourceConfiguration openSearchSourceConfiguration, Buffer<Record<Event>> buffer) {
        OpenPointInTimeResponse response = null;
        OpenPointInTimeRequest request = new OpenPointInTimeRequest.Builder().
                index(openSearchSourceConfiguration.getIndexParametersConfiguration().getInclude()).
                keepAlive(new Time.Builder().time(KEEP_ALIVE_VALUE).build()).build();
            LOG.info("Request is : {} ", request);
            try {
                response = elasticsearchClient.openPointInTime(request);
                LOG.info("Response is {} ",response);
            } catch (Exception ex){
                LOG.error(" {}",ex.getMessage());
            }
        //return response.id();
    }
    @Override
    public String searchPitIndexes(final OpenSearchSourceConfiguration openSearchSourceConfiguration, Buffer<Record<Event>> buffer) {
        SearchResponse<ObjectNode> searchResponse = null;
        try {
            searchResponse = elasticsearchClient.search(req ->
                            req.index(openSearchSourceConfiguration.getIndexParametersConfiguration().getInclude()),
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
    public void generateScrollId(final OpenSearchSourceConfiguration openSearchSourceConfiguration,Buffer<Record<Event>> buffer) {
        SearchResponse response = null;
        StringBuilder indexList = Utility.getIndexList(openSearchSourceConfiguration);
        SearchRequest searchRequest = SearchRequest
                .of(e -> e.index(indexList.toString()).size(openSearchSourceConfiguration.getSearchConfiguration().getBatchSize()).scroll(scr -> scr.time(TIME_VALUE)));
        try {
            response = elasticsearchClient.search(searchRequest, ObjectNode.class);
            LOG.info("Response is : {} ",response);
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        //return response.scrollId();
    }
    public co.elastic.clients.elasticsearch.core.ScrollRequest nextScrollRequest(final String scrollId) {
        return ScrollRequest
                .of(scrollRequest -> scrollRequest.scrollId(scrollId).scroll(Time.of(t -> t.time(TIME_VALUE))));
    }
    @Override
    public String searchScrollIndexes(final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        return null;
    }

    private boolean deleteScrollId(String id) throws IOException {
        ClearScrollRequest scrollRequest=new ClearScrollRequest.Builder().scrollId(id).build();
        ClearScrollResponse clearScrollResponse = elasticsearchClient.clearScroll(scrollRequest);
        LOG.info("Delete Scroll ID Response "+clearScrollResponse);
        return  clearScrollResponse.succeeded();
    }

    private boolean deletePitId(String id) throws IOException {
        ClosePointInTimeRequest request = new ClosePointInTimeRequest.Builder().id(id).build();
        ClosePointInTimeResponse closePointInTimeResponse = elasticsearchClient.closePointInTime(request);
        LOG.info("Delete PIT ID Response " + closePointInTimeResponse);
        return closePointInTimeResponse.succeeded();
    }

    private SearchResponse getSearchForSort(final OpenSearchSourceConfiguration openSearchSourceConfiguration, long searchAfter) {

        SearchResponse response = null;
        SearchRequest searchRequest = null;

        List<SortOptions> sortOptionsList = new ArrayList<>();
        StringBuilder indexList = Utility.getIndexList(openSearchSourceConfiguration);
        LOG.info("indexList: " + indexList);
        for(int sortIndex = 0 ; sortIndex < openSearchSourceConfiguration.getSearchConfiguration().getSorting().size() ; sortIndex++) {
            String sortOrder = openSearchSourceConfiguration.getSearchConfiguration().getSorting().get(sortIndex).getSortKey();
            SortOrder order = sortOrder.toLowerCase().equalsIgnoreCase("asc") ? SortOrder.Asc : SortOrder.Desc;
            int finalSortIndex = sortIndex;
            SortOptions sortOptions = new SortOptions.Builder().field(f -> f.field(openSearchSourceConfiguration.getSearchConfiguration().getSorting().get(finalSortIndex).getSortKey()).order(order)).build();
            sortOptionsList.add(sortOptions);
        }
        if (!openSearchSourceConfiguration.getQueryParameterConfiguration().getFields().isEmpty()) {
            String[] queryParam = openSearchSourceConfiguration.getQueryParameterConfiguration().getFields().get(0).split(":");
            searchRequest = SearchRequest
                    .of(e -> e.index(indexList.toString()).size(SEARCH_AFTER_SIZE).query(q -> q.match(t -> t
                                    .field(queryParam[0].trim())
                                    .query(queryParam[1].trim()))).searchAfter(s -> s.stringValue(String.valueOf(searchAfter)))
                            .sort(sortOptionsList));
        } else {
            searchRequest = SearchRequest
                    .of(e -> e.index(indexList.toString()).size(SEARCH_AFTER_SIZE).searchAfter(s -> s.stringValue(String.valueOf(searchAfter)))
                            .sort(sortOptionsList));
        }
        try {
            response = elasticsearchClient.search(searchRequest, JSONObject.class);
            LOG.info("Response of getSearchForSort : {} ", response);
        } catch (ElasticsearchException ese) {
           /* if (HttpStatus.SC_GATEWAY_TIMEOUT == ese.status() || HttpStatus.SC_INTERNAL_SERVER_ERROR == ese.status()) {
                LOG.info("Block to retry after sometime");
                BackoffService backoff = new BackoffService(retry, defaultTimeToWait);
                backoff.waitUntilNextTry();
                while (backoff.shouldRetry()) {
                    try {
                        response = client.search(searchRequest, JSONObject.class);
                    } catch (IOException ex) {
                        LOG.error(ex.getMessage());
                    }
                    if (HttpStatus.SC_GATEWAY_TIMEOUT != ese.status() && HttpStatus.SC_INTERNAL_SERVER_ERROR != ese.status()) {
                        backoff.doNotRetry();
                        break;
                    } else {
                        LOG.info("** Retrying after " + defaultTimeToWait + "mm **");
                        backoff.errorOccured();
                    }
                }
            }*/
        }catch(IOException e) {
            LOG.error(e.getMessage());
        }
        return response;

    }

    public void searchPitIndexesForPagination(final OpenSearchSourceConfiguration openSearchSourceConfiguration, final ElasticsearchClient client, long currentSearchAfterValue, Buffer<Record<Event>> buffer) throws TimeoutException {
        int batchSize = openSearchSourceConfiguration.getSearchConfiguration().getBatchSize();
        SearchResponse response = getSearchForSort(openSearchSourceConfiguration,currentSearchAfterValue);
        currentSearchAfterValue = extractSortValue(response, buffer);
        if(currentSearchAfterValue != 0) {
            searchPitIndexesForPagination(openSearchSourceConfiguration, client, currentSearchAfterValue,buffer);
        }
        else {
            LOG.info("---------- END OF PAGINATION MECHANISM -------------");
        }
    }

    private long extractSortValue(SearchResponse response, Buffer<Record<Event>> buffer) throws TimeoutException {
        HitsMetadata hitsMetadata = response.hits();
        int size = hitsMetadata.hits().size();
        long sortValue = 0;
        if(size != 0) {
            try {
                sortValue = ((Hit<Object>) hitsMetadata.hits().get(size - 1)).sort().get(0).longValue();
                LOG.info("extractSortValue : " + sortValue);
            }catch(Exception e){
                LOG.error(e.getMessage());
            }
        }
        sourceInfoProvider.writeClusterDataToBuffer(response.fields().toString(),buffer);
        return sortValue;
    }
}
