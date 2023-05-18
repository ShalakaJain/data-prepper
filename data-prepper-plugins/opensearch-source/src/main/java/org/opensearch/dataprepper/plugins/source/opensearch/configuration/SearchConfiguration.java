/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class SearchConfiguration {

    @JsonProperty("batch_size")
    private Integer batchSize;

    @JsonProperty("expand_wildcards")
    private WildCardOptions expandWildcards = WildCardOptions.ALL;

    @JsonProperty("sorting")
    private List<SortingConfiguration> sorting;

    public Integer getBatchSize() {
        return batchSize;
    }

    public WildCardOptions getExpandWildcards() {
        return expandWildcards;
    }

    public List<SortingConfiguration> getSorting() {
        return sorting;
    }
    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public void setExpandWildcards(WildCardOptions expandWildcards) {
        this.expandWildcards = expandWildcards;
    }

    public void setSorting(List<SortingConfiguration> sorting) {
        this.sorting = sorting;
    }
}
