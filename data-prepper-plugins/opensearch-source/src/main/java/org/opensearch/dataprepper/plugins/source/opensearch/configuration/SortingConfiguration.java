/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class SortingConfiguration {

    @JsonProperty("sort_key")
    private List<SortingKeyOrderConfiguration> sortKey;

    public List<SortingKeyOrderConfiguration> getSortKey() {
        return sortKey;
    }

}
