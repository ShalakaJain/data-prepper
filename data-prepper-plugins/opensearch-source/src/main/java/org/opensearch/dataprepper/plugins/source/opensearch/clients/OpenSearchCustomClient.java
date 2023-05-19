package org.opensearch.dataprepper.plugins.source.opensearch.clients;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

public class OpenSearchCustomClient {

    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchCustomClient.class);

    public <T> T execute(Class<T> responseType, String uri, Object request, String method) throws IOException {
        StringEntity requestEntity = new StringEntity(new ObjectMapper().writeValueAsString(request));
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
