/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SearchConfiguration {

    @JsonProperty("batch_size")
    private Integer batchSize;

    @JsonProperty("expand_wildcards")
    private WildCardConfiguration expandWildcards = WildCardConfiguration.ALL;

    @JsonProperty("sorting")
    private SortingConfiguration sorting;

    public Integer getBatchSize() {
        return batchSize;
    }

    public WildCardConfiguration getExpandWildcards() {
        return expandWildcards;
    }

    public SortingConfiguration getSorting() {
        return sorting;
    }
}
