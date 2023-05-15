package org.opensearch.dataprepper.plugins.source.opensearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.json.simple.parser.ParseException;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.cat.indices.IndicesRecord;
import org.opensearch.client.opensearch.core.ClearScrollRequest;
import org.opensearch.client.opensearch.core.ClearScrollResponse;
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

    private static final String POINT_IN_TIME_ID ="pit_id";

    private OpenSearchClient client ;
    private SourceInfoProvider sourceInfoProvider = new SourceInfoProvider();

    public OpenSearchApiCalls(OpenSearchClient openSearchClient) {
        this.client = openSearchClient;
    }

    @Override
    public void generatePitId(final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
            try {
                    Map pitResponse ;
                    String pitId = null;
                    PITRequest pitRequest = new PITRequest(new PITBuilder());
                    int countIndices = sourceInfoProvider.getCatIndices(openSearchSourceConfiguration,client).size();
                    List<IndicesRecord> indicesList = sourceInfoProvider.getCatIndices(openSearchSourceConfiguration,client);
                    for(int count= 0 ; count < countIndices ; count++) {
                        pitRequest.setIndex(new StringBuilder(indicesList.get(count).index()));
                        Map<String, String> params = new HashMap<>();
                        params.put(POINT_IN_TIME_KEEP_ALIVE, KEEP_ALIVE_VALUE);
                        pitRequest.setQueryParameters(params);
                        pitResponse = client._transport().performRequest(pitRequest, PITRequest.ENDPOINT, client._transportOptions());
                        LOG.info("PIT Response is : {}  ", pitResponse);
                        pitId = pitResponse.get(POINT_IN_TIME_ID).toString();
                        searchPitIndexes( pitId, openSearchSourceConfiguration);
                        deletePitId(pitId);
                    }
            } catch (Exception e){

            }
    }
    @Override
    public String searchPitIndexes(String pitId, OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        return "getResponseBody";
    }

    @Override
    public String generateScrollId(final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        ScrollRequest scrollRequest = new ScrollRequest(new ScrollBuilder());
        StringBuilder indexList = Utility.getIndexList(openSearchSourceConfiguration);
        scrollRequest.setIndex(indexList);
        scrollRequest.setBatchSize(BATCH_SIZE_VALUE);
        Map<String, Object> response = null;
        try
        {
           response =  client._transport().performRequest(scrollRequest,ScrollRequest.ENDPOINT,client._transportOptions());
           LOG.debug("Response is {}  " , response);
        }
       catch (Exception e)
       {
           LOG.error("Error occured "+e);
       }
        return response.toString();
    }
    @Override
    public String searchScrollIndexes(final OpenSearchSourceConfiguration openSearchSourceConfiguration) {

        return "responseBody";
    }

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

    private void deletePitId(String pitId) throws IOException {
        Map<String, String> inputMap = new HashMap<>();
        inputMap.put(POINT_IN_TIME_ID, pitId);
        LOG.debug("Request Object "+inputMap);
        Map executeResponse = execute(Map.class, "_search/point_in_time", inputMap,"DELETE");
        LOG.debug("Delete Pit ID Response " + executeResponse);
        List<Map> pits = (List<Map>) executeResponse.get("pits");
        LOG.debug("Delete successful "+ pits.get(0).get("successful"));
    }


    private <T> T execute(Class<T> responseType, String uri, Map<String,String> inputMap, String method) throws IOException {
        StringEntity requestEntity = new StringEntity(new ObjectMapper().writeValueAsString(inputMap));
        final String finalURL = "http://localhost:9200/" + uri;
        LOG.debug("Final URL "+finalURL);
        URI httpUri = URI.create(finalURL);
        HttpUriRequestBase operationRequest = new HttpUriRequestBase(method, httpUri);
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

        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(result.toString(), responseType);
    }

    @Deprecated
    private CloseableHttpResponse getCloseableHttpResponse( HttpUriRequestBase operationRequest) throws IOException {
        CloseableHttpClient httpClient= HttpClients.createDefault();
        CloseableHttpResponse pitResponse = httpClient.execute(operationRequest);
        LOG.debug("Pit Response "+pitResponse);
        return pitResponse;
    }
}
