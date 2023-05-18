package org.opensearch.dataprepper.plugins.source.opensearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.json.simple.parser.ParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.stubbing.OngoingStubbing;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SchedulingParameterConfiguration;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class SourceInfoProviderTests {

    HttpURLConnection con = null;

    @InjectMocks
    SourceInfoProvider sourceInfoProvider;

    @Mock
    OpenSearchSourceConfiguration openSearchSourceConfiguration;

    private static final String CLUSTER_STATS_ENDPOINTS = "_cluster/stats";

    private static final String GET_REQUEST_MEHTOD = "GET";

    private static final String CONTENT_TYPE = "content-type";

    private static final String CONTENT_TYPE_VALUE = "application/json";

    URL obj = null;

    @Mock
    private OpenSearchClient osClient;

    @Mock
    private ElasticsearchClient esClient;

    @Mock
    private Buffer buffer;

    @BeforeEach
    public void setup() {
        initMocks(this);
        con = mock(HttpURLConnection.class);
        when(openSearchSourceConfiguration.getMaxRetries()).thenReturn(5);
        when(openSearchSourceConfiguration.getHosts()).thenReturn(new ArrayList<String>(Arrays.asList("http://localhost:9200/")));
        SchedulingParameterConfiguration schedulingParameters = mock(SchedulingParameterConfiguration.class);
        when(schedulingParameters.getRate()).thenReturn(Duration.parse("PT8H"));
        when(schedulingParameters.getStartTime()).thenReturn(LocalDateTime.now());
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
        obj =  new URL(openSearchSourceConfiguration.getHosts().get(0));
        SourceInfo sourceInfo = new SourceInfo();
        sourceInfo.setOsVersion("7.9.0");
        sourceInfo.setDataSource("elasticsearch");
        sourceInfoProvider.versionCheckForElasticSearch(openSearchSourceConfiguration,sourceInfo,esClient,buffer);
    }
}
