/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class SearchConfigurationTest {


    private ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory().enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS));

    @Test
    public void search_config_values_test() throws JsonProcessingException {

        final String searchConfigurationYaml =
                "  batch_size: 1000\n" +
                "  expand_wildcards: \"open\"";
        final SearchConfiguration searchConfiguration = objectMapper.readValue(searchConfigurationYaml, SearchConfiguration.class);
        assertThat(searchConfiguration.getExpandWildcards(),equalTo(WildCardConfiguration.OPEN));
        assertThat(searchConfiguration.getBatchSize(),equalTo(1000));
    }
}
