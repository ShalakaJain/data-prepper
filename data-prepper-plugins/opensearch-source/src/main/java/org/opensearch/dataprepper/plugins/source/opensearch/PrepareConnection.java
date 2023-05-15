package org.opensearch.dataprepper.plugins.source.opensearch;

import org.apache.http.HttpHeaders;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.core5.http.HttpHost;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.elasticsearch.client.RestClient;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PrepareConnection {

   private static final String HEADER_NAME = "X-Elastic-Product";

   private static final String HEADER_VALUE = "Elasticsearch";

   public OpenSearchClient prepareOpensearchConnection(OpenSearchSourceConfiguration openSearchSourceConfiguration) {
     // getHostDetails(openSearchSourceConfiguration.getHosts().get(0));
      final HttpHost host = new HttpHost("http", "localhost", 9200);
      final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      final OpenSearchTransport transport = ApacheHttpClient5TransportBuilder
              .builder(host)
              .setMapper(new org.opensearch.client.json.jackson.JacksonJsonpMapper())
              .build();
      return new OpenSearchClient(transport);
   }

   public ElasticsearchClient prepareElasticSearchConnection() {
      RestClient client = org.elasticsearch.client.RestClient.builder(new org.apache.http.HttpHost("localhost", 9200)).
              setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                      .setDefaultHeaders(List.of(new BasicHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString())))
                      .addInterceptorLast((HttpResponseInterceptor) (response, context) -> response.addHeader(HEADER_NAME, HEADER_VALUE))).build();
      JacksonJsonpMapper jacksonJsonpMapper = new JacksonJsonpMapper();
      ElasticsearchTransport transport = new RestClientTransport(client, jacksonJsonpMapper);
      return new ElasticsearchClient(transport);
   }

   private String[] getHostDetails(String url) throws MalformedURLException {
      String[] hostDetails = new String[3];
      URL urlLink = new URL(url);
      hostDetails[0] = urlLink.getProtocol();
      hostDetails[1] = urlLink.getHost();
      hostDetails[2] = String.valueOf(urlLink.getPort());
      return hostDetails;
   }
}


