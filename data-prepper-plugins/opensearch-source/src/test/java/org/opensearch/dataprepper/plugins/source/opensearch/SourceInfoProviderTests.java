package org.opensearch.dataprepper.plugins.source.opensearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.json.simple.parser.ParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SchedulingParameterConfiguration;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class SourceInfoProviderTests {
    private static final String CLUSTER_STATS_ENDPOINTS = "_cluster/stats";
    private static final String GET_REQUEST_MEHTOD = "GET";
    private static final String CONTENT_TYPE = "content-type";
    private static final String CONTENT_TYPE_VALUE = "application/json";
    URL obj = null;
    HttpURLConnection con = null;
    @InjectMocks
    SourceInfoProvider sourceInfoProvider;
    @Mock
    OpenSearchSourceConfiguration openSearchSourceConfiguration;
    @Mock
    private OpenSearchClient osClient;
    @Mock
    private Buffer buffer;

    @Mock
    private OpenSearchApiCalls openSearchApiCalls;

    @Mock
    ElasticSearchApiCalls elasticSearchApiCalls;

    @Mock
    ElasticsearchClient esClient;

    @BeforeEach
    public void setup() {
        initMocks(this);
        con = mock(HttpURLConnection.class);
        when(openSearchSourceConfiguration.getMaxRetries()).thenReturn(5);
        when(openSearchSourceConfiguration.getHosts()).thenReturn(new ArrayList<String>(Arrays.asList("http://localhost:9200/")));
        SchedulingParameterConfiguration schedulingParameters = mock(SchedulingParameterConfiguration.class);
        //when(schedulingParameters.getRate()).thenReturn(new Duration());
        //when(schedulingParameters.getStartTime()).thenReturn(new Date());
        when(schedulingParameters.getJobCount()).thenReturn(3);
        when(openSearchSourceConfiguration.getSchedulingParameterConfiguration()).thenReturn(schedulingParameters);
    }
    @Test
    public void testCheckStatus() throws IOException, ParseException {
        obj =  new URL(openSearchSourceConfiguration.getHosts().get(0) + CLUSTER_STATS_ENDPOINTS);
        SourceInfo sourceInfo = new SourceInfo();
        sourceInfo = sourceInfoProvider.checkStatus(openSearchSourceConfiguration,sourceInfo);
        assertThat(sourceInfo, notNullValue());
        assertTrue(sourceInfo.getHealthStatus());
        assertThat(sourceInfo.getOsVersion(), notNullValue());
    }

    @Test
    public void testGetsourceInfo() throws IOException, ParseException {
        obj =  new URL(openSearchSourceConfiguration.getHosts().get(0));
        SourceInfo sourceInfo = new SourceInfo();
        String res = sourceInfoProvider.getSourceInfo(openSearchSourceConfiguration);
        assertThat(res, notNullValue());
    }

    @Test
    public void testOpenSearchVersionCheck() throws IOException, ParseException, TimeoutException {
        obj =  new URL(openSearchSourceConfiguration.getHosts().get(0));
        SourceInfo sourceInfo = new SourceInfo();
        sourceInfo.setOsVersion("1.6.0");
        sourceInfo.setDataSource("opensearch");
        sourceInfoProvider.versionCheckForOpenSearch(openSearchSourceConfiguration,sourceInfo,osClient,buffer);
    }

    @Test
    public void testElasticVersionCheck() throws IOException, ParseException, TimeoutException {
        obj = new URL(openSearchSourceConfiguration.getHosts().get(0));
        SourceInfo sourceInfo = new SourceInfo();
        sourceInfo.setOsVersion("7.9.0");
        sourceInfo.setDataSource("elasticsearch");
        sourceInfoProvider.versionCheckForElasticSearch(openSearchSourceConfiguration, sourceInfo, esClient, buffer);
    }
/*    @Test
    public void testversionCheckForOpenSearchForLowerVersion() throws IOException, ParseException, TimeoutException {
        obj =  new URL(openSearchSourceConfiguration.getHosts().get(0));
        SourceInfo sourceInfo = new SourceInfo();
        sourceInfo.setOsVersion("129");
        sourceInfo.setDataSource("opensearch");
        MockedStatic utility = mockStatic(Utility.class);
        Mockito.doNothing().when(openSearchApiCalls).generateScrollId(openSearchSourceConfiguration,buffer);
        sourceInfoProvider.versionCheckForOpenSearch(openSearchSourceConfiguration,sourceInfo,osClient,buffer);
    }

    @Test
    public void testversionCheckForOpenSearchForHigherVersion() throws IOException, ParseException, TimeoutException {
        obj =  new URL(openSearchSourceConfiguration.getHosts().get(0));
        SourceInfo sourceInfo = new SourceInfo();
        sourceInfo.setOsVersion("130");
        sourceInfo.setDataSource("opensearch");
        //MockedStatic utility = mockStatic(Utility.class);
        Mockito.doNothing().when(openSearchApiCalls).generatePitId(openSearchSourceConfiguration,buffer);
        sourceInfoProvider.versionCheckForOpenSearch(openSearchSourceConfiguration,sourceInfo,osClient,buffer);
    }*/
}
