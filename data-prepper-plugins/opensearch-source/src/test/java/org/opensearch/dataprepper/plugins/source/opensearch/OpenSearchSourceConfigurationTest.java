/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.Test;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.AwsAuthenticationConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.ConnectionConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SchedulingParameterConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.SearchConfiguration;
import org.opensearch.dataprepper.plugins.source.opensearch.configuration.WildCardConfiguration;
import software.amazon.awssdk.regions.Region;

import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class OpenSearchSourceConfigurationTest {

    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    public void open_search_source_config_values_test() throws JsonProcessingException {

        final String sourceConfigurationYaml = "connection:\n" +
                "  hosts: [\"http://localhost:9200\"]\n" +
                "  username: test\n" +
                "  password: test\n" +
                "  cert: \"cert\"\n" +
                "indices:\n" +
                "  include:\n" +
                "    - \"shakespeare\"\n" +
                "aws:\n" +
                "  region: \"us-east-1\"\n" +
                "  sts_role_arn: \"arn:aws:iam::123456789012:role/aos-role\"\n" +
                "scheduling:\n" +
                "  rate: \"P2DT3H4M\"\n" +
                "  job_count: 3\n" +
                "  start_time: 2023-05-05T18:00:00\n" +
                "query:\n" +
                "  fields: [\"test_variable : test_value\"]\n" +
                "search_options:\n" +
                "  batch_size: 1000\n" +
                "  expand_wildcards: \"open\"\n" +
                "  sorting:\n" +
                "retry: \n" +
                "  max_retries: 3";
        final OpenSearchSourceConfiguration sourceConfiguration = objectMapper.readValue(sourceConfigurationYaml, OpenSearchSourceConfiguration.class);
        final ConnectionConfiguration connectionConfig = sourceConfiguration.getConnectionConfiguration();
        final SearchConfiguration searchConfiguration = sourceConfiguration.getSearchConfiguration();
        final AwsAuthenticationConfiguration awsAuthenticationOptions = sourceConfiguration.getAwsAuthenticationOptions();
        final SchedulingParameterConfiguration schedulingParameterConfiguration = sourceConfiguration.getSchedulingParameterConfiguration();
        assertThat(awsAuthenticationOptions.getAwsRegion(),equalTo(Region.US_EAST_1));
        assertThat(connectionConfig.getHosts().get(0),equalTo("http://localhost:9200"));
        assertThat(connectionConfig.getUsername(),equalTo("test"));
        assertThat(connectionConfig.getPassword(),equalTo("test"));
        assertThat(connectionConfig.getCertPath(),equalTo(Path.of("cert")));
        assertThat(searchConfiguration.getExpandWildcards(),equalTo(WildCardConfiguration.OPEN));
        assertThat(searchConfiguration.getBatchSize(),equalTo(1000));
        assertThat(sourceConfiguration.getQueryParameterConfiguration().getFields(),equalTo(List.of("test_variable : test_value")));
        assertThat(schedulingParameterConfiguration.getRate(),equalTo(Duration.parse("P2DT3H4M")));
        assertThat(schedulingParameterConfiguration.getJobCount(),equalTo(3));
        assertThat(schedulingParameterConfiguration.getStartTime(),equalTo(LocalDateTime.parse("2023-05-05T18:00:00")));
        assertThat(sourceConfiguration.getIndexParametersConfiguration().getInclude().get(0),equalTo("shakespeare"));
        assertThat(sourceConfiguration.getRetryConfiguration().getMaxRetries(),equalTo(3));
    }
}