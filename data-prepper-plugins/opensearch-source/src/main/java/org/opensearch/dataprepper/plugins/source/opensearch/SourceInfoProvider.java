package org.opensearch.dataprepper.plugins.source.opensearch;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.cat.indices.IndicesRecord;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

public class SourceInfoProvider {

    private static final Logger LOG = LoggerFactory.getLogger(SourceInfoProvider.class);

    private String datasource;

    private static final String GET_REQUEST_MEHTOD = "GET";

    private static final String CONTENT_TYPE = "content-type";

    private static final String CONTENT_TYPE_VALUE = "application/json";

    private static final String VERSION = "version";

    private static final String DISTRIBUTION = "distribution";

    private static final String ELASTIC_SEARCH = "elasticsearch";

    private static final String CLUSTER_STATS_ENDPOINTS = "_cluster/stats";

    private static final String CLUSTER_HEALTH_STATUS = "status";

    private static final String CLUSTER_HEALTH_STATUS_RED = "red";

    private static final String NODES = "nodes";

    private static final String VERSIONS = "versions";

    private static final String REGULAR_EXPRESSION = "[^a-zA-Z0-9]";

    private static final String OPEN_SEARCH ="opensearch";

    private static final int VERSION_1_3_0 = 130;

    private  final JsonFactory jsonFactory = new JsonFactory();

    private static final ObjectMapper objectMapper = new ObjectMapper();

    HashMap<String, String> indexMap ;

    public String getSourceInfo(final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        try {
            JSONParser jsonParser = new JSONParser();
            StringBuilder response = new StringBuilder();
            if (StringUtils.isBlank(openSearchSourceConfiguration.getHosts().get(0)))
                throw new IllegalArgumentException("Hostname cannot be null or empty");
            URL obj = new URL(openSearchSourceConfiguration.getHosts().get(0));
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
            con.setRequestMethod(GET_REQUEST_MEHTOD);
            con.setRequestProperty(CONTENT_TYPE, CONTENT_TYPE_VALUE);
            int responseCode = con.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();
               LOG.info("Response is  : {} " , response);
            } else {
                LOG.error("GET request did not work.");
            }
            JSONObject jsonObject = (JSONObject) jsonParser.parse(String.valueOf(response));
            Map<String,String> versionMap = ((Map) jsonObject.get(VERSION));
            for (Map.Entry<String,String> entry : versionMap.entrySet())
            {
                if (entry.getKey().equals(DISTRIBUTION)) {
                    datasource = String.valueOf(entry.getValue());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (datasource == null)
            datasource = ELASTIC_SEARCH;
        return datasource;
    }
    public SourceInfo checkStatus(final OpenSearchSourceConfiguration openSearchSourceConfiguration,final SourceInfo sourceInfo) throws IOException, ParseException {
        String osVersion = null;
        URL obj = new URL(openSearchSourceConfiguration.getHosts().get(0) + CLUSTER_STATS_ENDPOINTS);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        con.setRequestMethod(GET_REQUEST_MEHTOD);
        con.setRequestProperty(CONTENT_TYPE, CONTENT_TYPE_VALUE);
        int responseCode = con.getResponseCode();
        JSONParser jsonParser = new JSONParser();
        StringBuilder response = new StringBuilder();
        String status;
        if (responseCode == HttpURLConnection.HTTP_OK) {
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            LOG.info("Response is {} " ,response);
        } else {
            LOG.info("GET request did not work.");
        }
        JSONObject jsonObject = (JSONObject) jsonParser.parse(String.valueOf(response));
        status = (String) jsonObject.get(CLUSTER_HEALTH_STATUS);
        if (status.equalsIgnoreCase(CLUSTER_HEALTH_STATUS_RED))
            sourceInfo.setHealthStatus(false);
        Map<String,String> nodesMap = ((Map) jsonObject.get(NODES));
        for (Map.Entry<String,String> entry : nodesMap.entrySet())
        {
            if (entry.getKey().equals(VERSIONS)) {
                osVersion = String.valueOf(entry.getValue());
                sourceInfo.setOsVersion(osVersion);
            }
        }
        LOG.info("version Number  : {} " , osVersion);
        return sourceInfo;
    }

    public void versionCheck(final OpenSearchSourceConfiguration openSearchSourceConfiguration, final SourceInfo sourceInfo, final OpenSearchClient client)  throws TimeoutException, IOException {
        int osVersionIntegerValue = Integer.parseInt(sourceInfo.getOsVersion().replaceAll(REGULAR_EXPRESSION, ""));
        // osVersionIntegerValue = 123; to test Scroll API
        if ((sourceInfo.getDataSource().equalsIgnoreCase(OPEN_SEARCH))
                && (osVersionIntegerValue >= VERSION_1_3_0)) {

            OpenSearchApiCalls openSearchApiCalls = new OpenSearchApiCalls(client);
            openSearchApiCalls.generatePitId(openSearchSourceConfiguration);

        } else if (sourceInfo.getDataSource().equalsIgnoreCase(OPEN_SEARCH) && (osVersionIntegerValue < VERSION_1_3_0)) {
            OpenSearchApiCalls openSearchApiCalls = new OpenSearchApiCalls(client);
            String scrollId = openSearchApiCalls.generateScrollId(openSearchSourceConfiguration);
            LOG.info("Scroll Response : {} ", scrollId);
            String getData = openSearchApiCalls.searchScrollIndexes(openSearchSourceConfiguration);
            LOG.info("Data in batches : {} ", getData);
            // writeClusterDataToBuffer(getData,buffer);
          /*  if (scrollId != null && !scrollId.isBlank()) {
                openSearchApiCalls.delete(scrollId, client, osVersionIntegerValue);
            }*/
        }
    }
    public void writeClusterDataToBuffer(final String responseBody,final Buffer<Record<Event>> buffer) throws TimeoutException {
        try {
            LOG.info("Write to buffer code started {} ",buffer);
            final JsonParser jsonParser = jsonFactory.createParser(responseBody);
            final Map<String, Object> innerJson = objectMapper.readValue(jsonParser, Map.class);
            Event event = JacksonLog.builder().withData(innerJson).build();
            Record<Event> jsonRecord = new Record<>(event);
            LOG.info("Data is pushed to buffer {} ",jsonRecord);
            buffer.write(jsonRecord, 1200);
        }
        catch (Exception e)
        {
            LOG.error("Unable to parse json data [{}], assuming plain text", responseBody, e);
            final Map<String, Object> plainMap = new HashMap<>();
            plainMap.put("message", responseBody);
            Event event = JacksonLog.builder().withData(plainMap).build();
            Record<Event> jsonRecord = new Record<>(event);
            buffer.write(jsonRecord, 1200);
        }
    }
    public List<IndicesRecord> callCatIndices(final OpenSearchClient client) throws IOException,ParseException {
        List<IndicesRecord> indexInfoList = client.cat().indices().valueBody();
        return indexInfoList;
    }

    public HashMap<String, String> getIndexMap(final List<IndicesRecord> indexInfos) {
        HashMap<String,String> indexMap = new HashMap<>();
        for(IndicesRecord indexInfo : indexInfos) {
            String indexname = indexInfo.index();
            String indexSize = indexInfo.storeSize();
            indexMap.put(indexname,indexSize);
        }
        return indexMap;
    }

    public static StringBuilder getIndexList(final OpenSearchSourceConfiguration openSearchSourceConfiguration)
    {
        List<String> include = openSearchSourceConfiguration.getIndexParametersConfiguration().getInclude();
        List<String> exclude = openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude();
        String includeIndexes = null;
        String excludeIndexes = null;
        StringBuilder indexList = new StringBuilder();
        if(!include.isEmpty())
            includeIndexes = include.stream().collect(Collectors.joining(","));
        if(!exclude.isEmpty())
            excludeIndexes = exclude.stream().collect(Collectors.joining(",-*"));
        indexList.append(includeIndexes);
        indexList.append(",-*"+excludeIndexes);
        return indexList;
    }

    public  List<IndicesRecord> getIndicesRecords(final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                                                  final List<IndicesRecord>  indicesRecords) {
        if (openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude() != null
                && !openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude().isEmpty()) {
            List<String> filteredIncludeIndexes = openSearchSourceConfiguration.getIndexParametersConfiguration().getInclude().stream()
                    .filter(index -> !(openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude().contains(index))).collect(Collectors.toList());
            openSearchSourceConfiguration.getIndexParametersConfiguration().setInclude(filteredIncludeIndexes);
        }
        openSearchSourceConfiguration.getIndexParametersConfiguration().getInclude().forEach(index -> {
            IndicesRecord indexRecord =
                    new IndicesRecord.Builder().index(index).build();
            indicesRecords.add(indexRecord);

        });
        return indicesRecords;
    }

    public List<IndicesRecord> getCatIndices(final OpenSearchSourceConfiguration openSearchSourceConfiguration, OpenSearchClient client) throws IOException, ParseException {
        List<IndicesRecord> catIndices = new ArrayList<>();
        if (openSearchSourceConfiguration.getIndexParametersConfiguration().getInclude() == null ||
                openSearchSourceConfiguration.getIndexParametersConfiguration().getInclude().isEmpty()) {
            catIndices = callCatIndices(client);

            //filtering out  based on exclude indices
            if (openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude() != null
                    && !openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude().isEmpty()) {
                catIndices = catIndices.stream().filter(c -> !(openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude().contains(c.index()))).
                        collect(Collectors.toList());
            }
        } else {
            catIndices = getIndicesRecords(openSearchSourceConfiguration, catIndices);
        }

        indexMap = getIndexMap(catIndices);
        LOG.info("Indexes  are {} :  ", indexMap);
        return catIndices;


    }
}