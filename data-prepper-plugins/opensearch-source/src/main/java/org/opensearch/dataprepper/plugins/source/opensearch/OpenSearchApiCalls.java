package org.opensearch.dataprepper.plugins.source.opensearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.OpenSearchException;
import org.opensearch.client.opensearch.cat.indices.IndicesRecord;
import org.opensearch.client.opensearch.core.ClearScrollRequest;
import org.opensearch.client.opensearch.core.ClearScrollResponse;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

public class OpenSearchApiCalls implements SearchAPICalls {

    private static final String POINT_IN_TIME_KEEP_ALIVE = "keep_alive";

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchApiCalls.class);

    private static final String KEEP_ALIVE_VALUE = "24h";

    private static final Integer BATCH_SIZE_VALUE = 1000;

    private static final int OPEN_SEARCH_VERSION = 130;

    private static final String POINT_IN_TIME_ID ="id";

    private static final String POINT_IN_TIME = "pit";

    private static final String PIT_ID = "pit_id";

    private OpenSearchClient client ;

    private SourceInfoProvider sourceInfoProvider = new SourceInfoProvider();

    private int currentBatchSize = 0;

    private JSONObject pitSearchRequestObj = new JSONObject();

    private StringBuffer stringEntity ;

    public OpenSearchApiCalls(OpenSearchClient openSearchClient)
    {
        this.client = openSearchClient;

    }

    private ObjectMapper objectMapper = new ObjectMapper();

    private Map pitResponse ;

    private PITRequest pitRequest;

    private CloseableHttpResponse pitCloseableResponse = null;

    @Override
    public void generatePitId(final OpenSearchSourceConfiguration openSearchSourceConfiguration , Buffer<Record<Event>> buffer) throws IOException {
            try {
                    System.out.println("Start PIT Process");
                    String pitId = null;
                    pitRequest = new PITRequest(new PITBuilder());
                    int countIndices = sourceInfoProvider.getCatIndicesOpenSearch(openSearchSourceConfiguration,client).size();
                    List<IndicesRecord> indicesList = sourceInfoProvider.getCatIndicesOpenSearch(openSearchSourceConfiguration,client);
                    for(int count= 0 ; count < countIndices ; count++) {
                        pitRequest.setIndex(new StringBuilder(indicesList.get(count).index()));
                        Map<String, String> params = new HashMap<>();
                        params.put(POINT_IN_TIME_KEEP_ALIVE, KEEP_ALIVE_VALUE);
                        pitRequest.setQueryParameters(params);
                        LOG.info("pitRequest" +pitRequest.toString());
                        LOG.info("client._transportOptions()" + client );
                        pitResponse = client._transport().performRequest(pitRequest, PITRequest.ENDPOINT, client._transportOptions());
                        System.out.println("PIT Response is : {}  "+pitResponse);
                        LOG.info("PIT Response is : {}  ", pitResponse );
                        pitId = pitResponse.get(PIT_ID).toString();
                        searchPitIndexes(pitId, openSearchSourceConfiguration , buffer);
                        deletePitId(pitId,openSearchSourceConfiguration);
                    }
            } catch (OpenSearchException ose){
                if(HttpStatus.SC_GATEWAY_TIMEOUT == ose.status() || HttpStatus.SC_INTERNAL_SERVER_ERROR == ose.status()) {
                    LOG.info("Block to retry after sometime");
                    BackoffService backoff = new BackoffService(openSearchSourceConfiguration.getMaxRetries());
                    backoff.waitUntilNextTry();
                    while (backoff.shouldRetry()) {
                        pitResponse = client._transport().performRequest(pitRequest, PITRequest.ENDPOINT, client._transportOptions());
                        if (pitResponse != null && pitResponse.size()>0) {
                            backoff.doNotRetry();
                            break;
                        } else {
                            LOG.info("** Retrying ** ");
                            backoff.errorOccured();
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
    }
    @Override
    public void searchPitIndexes(final String pitId ,final OpenSearchSourceConfiguration openSearchSourceConfiguration ,Buffer<Record<Event>> buffer) {
        try
        {
            int sizeForPagination = 20;
            CloseableHttpClient httpClient= HttpClients.createDefault();
            HttpGet httpGet=new HttpGet(openSearchSourceConfiguration.getHosts().get(0)+"_search");
            httpGet.setHeader("Accept", ContentType.APPLICATION_JSON);
            httpGet.setHeader("Content-type",ContentType.APPLICATION_JSON);
            if( BATCH_SIZE_VALUE > openSearchSourceConfiguration.getSearchConfiguration().getBatchSize()) {
                System.out.println("Not to Paginate");
                currentBatchSize = openSearchSourceConfiguration.getSearchConfiguration().getBatchSize();
                pitSearchRequestObj.put("size", currentBatchSize);
                if (openSearchSourceConfiguration.getQueryParameterConfiguration() != null && openSearchSourceConfiguration.getQueryParameterConfiguration().getFields() != null) {
                    JSONObject matchObj = new JSONObject();
                    String[] queryObj = openSearchSourceConfiguration.getQueryParameterConfiguration().getFields().get(0).split(":");
                    matchObj.put(queryObj[0].trim(), queryObj[1].trim());
                    JSONObject queryInnerObj = new JSONObject();
                    queryInnerObj.put("match", matchObj);
                    pitSearchRequestObj.put("query", queryInnerObj);
                }
                JSONObject pitObj = new JSONObject();
                pitObj.put(POINT_IN_TIME_ID, pitId);
                pitObj.put(POINT_IN_TIME_KEEP_ALIVE, KEEP_ALIVE_VALUE);
                pitSearchRequestObj.put(POINT_IN_TIME, pitObj);

                System.out.println("print request : " + pitSearchRequestObj.toString());
                StringEntity stringEntity = new StringEntity(pitSearchRequestObj.toString());
                httpGet.setEntity(stringEntity);
                sourceInfoProvider.writeClusterDataToBuffer(stringEntity.toString(), buffer);

            } else {
                //paginate
                int i =0;
                List<Integer> searchAfter = null;
                System.out.println("Paginate");
                currentBatchSize = openSearchSourceConfiguration.getSearchConfiguration().getBatchSize();
                while(currentBatchSize > 0 ) {
                    LOG.info("Paginate " + i++ + "current size "+currentBatchSize);
                    stringEntity = handlePitSearchRequest(openSearchSourceConfiguration, pitId, sizeForPagination, searchAfter, httpClient, httpGet);
                    PitSearchResponse pitSearchResponse = objectMapper.readValue(stringEntity.toString(), PitSearchResponse.class);
                    List<Hit> hit = pitSearchResponse.getHits().getHits();
                    Hit lastHit = new Hit();
                    if (!hit.isEmpty())
                        lastHit = hit.get(hit.size() - 1);
                    searchAfter = lastHit.getSort();
                    LOG.info(searchAfter.toString());
                    sourceInfoProvider.writeClusterDataToBuffer(stringEntity.toString(), buffer);
                    currentBatchSize = currentBatchSize - sizeForPagination;
                }
            }
        } catch (Exception e){
           /* if(HttpStatus.SC_GATEWAY_TIMEOUT == pitCloseableResponse.getCode() || HttpStatus.SC_INTERNAL_SERVER_ERROR == pitCloseableResponse.getCode()) {
                LOG.info("Block to retry after sometime");
                BackoffService backoff = new BackoffService(openSearchSourceConfiguration.getMaxRetries());
                backoff.waitUntilNextTry();
                while (backoff.shouldRetry()) {
                    pitResponse = httpAsyncClient.execute(httpDelete);
                    if (HttpStatus.SC_GATEWAY_TIMEOUT != pitResponse.getCode() && HttpStatus.SC_INTERNAL_SERVER_ERROR != pitResponse.getCode()) {
                        backoff.doNotRetry();
                        break;
                    } else {
                        LOG.info("** Retrying **");
                        backoff.errorOccured();
                    }
                }
            }*/
        }
    }

    @Override
    public void getScrollResponse(final OpenSearchSourceConfiguration openSearchSourceConfiguration ,Buffer<Record<Event>> buffer) {
        ScrollRequest scrollRequest = new ScrollRequest(new ScrollBuilder());
        StringBuilder indexList = Utility.getIndexList(openSearchSourceConfiguration);
        scrollRequest.setIndex(indexList);
        scrollRequest.setBatchSize(BATCH_SIZE_VALUE);
        Map<String, Object> response = null;
        try
        {
           response =  client._transport().performRequest(scrollRequest,ScrollRequest.ENDPOINT,client._transportOptions());
           LOG.debug("Response is {}  " , response);
           sourceInfoProvider.writeClusterDataToBuffer(response.toString(),buffer);
        }
       catch (Exception e)
       {
           LOG.error("Error occured "+e);
       }
    }
  /*  @Override
    public String searchScrollIndexes(final OpenSearchSourceConfiguration openSearchSourceConfiguration) {

        return "responseBody";
    }*/

    /*@Override
    public void delete(final String id, final Integer openSearchVersion) {
        LOG.info("PIT or Scroll ID to be deleted - " + id);
        try {
            if (openSearchVersion.intValue() >= OPEN_SEARCH_VERSION) {
                 deletePitId(id);
            } else {
                 deleteScrollId(id, client);

            }

        } catch (IOException e) {
            LOG.error("Error occured while closing PIT " + e);
        }

    }*/

    private void deleteScrollId(String id, OpenSearchClient client) throws IOException {
        ClearScrollRequest scrollRequest=new ClearScrollRequest.Builder().scrollId(id).build();
        ClearScrollResponse clearScrollResponse = client.clearScroll(scrollRequest);
        LOG.debug("Delete Scroll ID Response "+clearScrollResponse);
        LOG.debug("Delete successful "+ clearScrollResponse.succeeded());
    }

    private void deletePitId(String pitId , OpenSearchSourceConfiguration openSearchSourceConfiguration) throws IOException {
        LOG.info("Delete successful "+ pitId);
        Map<String, String> inputMap = new HashMap<>();
        inputMap.put(PIT_ID, pitId);
        LOG.info("Request Object "+inputMap);
        Map executeResponse = execute(Map.class, "_search/point_in_time", inputMap,"DELETE" , openSearchSourceConfiguration);
        LOG.info("Delete Pit ID Response " + executeResponse);
        List<Map> pits = (List<Map>) executeResponse.get("pits");
        LOG.info("Delete successful "+ pits.get(0).get("successful"));
    }


    public StringBuffer handlePitSearchRequest(OpenSearchSourceConfiguration openSearchSourceConfiguration , String pitId , int size , List<Integer> searchAfter , CloseableHttpClient httpClient , HttpGet httpGet) throws IOException {

        pitSearchRequestObj.put("size", size );
        if (openSearchSourceConfiguration.getQueryParameterConfiguration() != null && openSearchSourceConfiguration.getQueryParameterConfiguration().getFields() != null) {
            JSONObject matchObj = new JSONObject();
            String[] queryObj = openSearchSourceConfiguration.getQueryParameterConfiguration().getFields().get(0).split(":");
            matchObj.put(queryObj[0].trim(), queryObj[1].trim());
            JSONObject queryInnerObj = new JSONObject();
            queryInnerObj.put("match", matchObj);
            pitSearchRequestObj.put("query", queryInnerObj);
        }
        JSONObject pitObj = new JSONObject();
        pitObj.put(POINT_IN_TIME_ID, pitId);
        pitObj.put(POINT_IN_TIME_KEEP_ALIVE, KEEP_ALIVE_VALUE);
        pitSearchRequestObj.put(POINT_IN_TIME, pitObj);

        JSONArray sortObj = new JSONArray();
        for (int sortIndex = 0 ; sortIndex < openSearchSourceConfiguration.getSearchConfiguration().getSorting().size() ; sortIndex++) {
                String sortKey = openSearchSourceConfiguration.getSearchConfiguration().getSorting().get(sortIndex).getSortKey();
                String sortOrder = openSearchSourceConfiguration.getSearchConfiguration().getSorting().get(sortIndex).getOrder();
                JSONObject sortInnerObj = new JSONObject();
                sortInnerObj.put(sortKey.trim(),sortOrder);
                sortObj.add(sortInnerObj);
        }
        pitSearchRequestObj.put("sort",sortObj);

        if(searchAfter!=null && !searchAfter.isEmpty()) {
            pitSearchRequestObj.put("search_after",searchAfter);
        }

        System.out.println("print request : " + pitSearchRequestObj.toString());
        StringEntity stringEntity = new StringEntity(pitSearchRequestObj.toString());
        currentBatchSize = currentBatchSize-size;
        LOG.info(" current batch Size "+currentBatchSize);
        httpGet.setEntity(stringEntity);
        return getSearchResponse(httpClient,httpGet);
    }

    private <T> T execute(Class<T> responseType, String uri, Map<String,String> inputMap, String httpMethod , OpenSearchSourceConfiguration openSearchSourceConfiguration) throws IOException {
        StringEntity requestEntity = new StringEntity(new ObjectMapper().writeValueAsString(inputMap));
        URI httpUri = URI.create(openSearchSourceConfiguration.getHosts().get(0));
        HttpUriRequestBase operationRequest = new HttpUriRequestBase(httpMethod, httpUri);
        operationRequest.setHeader("Accept", ContentType.APPLICATION_JSON);
        operationRequest.setHeader("Content-type", ContentType.APPLICATION_JSON);
        operationRequest.setEntity(requestEntity);
        CloseableHttpResponse pitResponse = getCloseableHttpResponse(operationRequest);
        BufferedReader reader = new BufferedReader(new InputStreamReader(pitResponse.getEntity().getContent()));
        StringBuffer result = new StringBuffer();
        String line = "";
        while ((line = reader.readLine()) != null) {
            result.append(line);
        }
        return objectMapper.readValue(result.toString(), responseType);
    }

    @Deprecated
    private CloseableHttpResponse getCloseableHttpResponse( HttpUriRequestBase operationRequest) throws IOException {
        CloseableHttpClient httpClient= HttpClients.createDefault();
        CloseableHttpResponse pitResponse = httpClient.execute(operationRequest);
        LOG.debug("Pit Response "+pitResponse);
        return pitResponse;
    }

    private String getSearchAfterParameters(StringEntity stringEntity) throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        PitSearchResponse pitSearchResponse = objectMapper.readValue(stringEntity.toString(), PitSearchResponse.class);
        List<Hit> hit =  pitSearchResponse.getHits().getHits();
        Hit lastHit = new Hit();
        if (!hit.isEmpty())
            lastHit = hit.get(hit.size()-1);
        List<Integer> searchAfter = lastHit.getSort();
        LOG.info(searchAfter.toString());
        return null;
    }

    private StringBuffer getSearchResponse(CloseableHttpClient httpClient, HttpGet httpGet) throws IOException {
        StringBuffer result = new StringBuffer();
        CloseableHttpResponse pitCloseableResponse = httpClient.execute(httpGet);
        BufferedReader reader = new BufferedReader(new InputStreamReader(pitCloseableResponse.getEntity().getContent()));
        String line = "";
        while ((line = reader.readLine()) != null) {
            result.append(line);
        }
        LOG.info("Search response :  " + result);
        return result;
    }
}
