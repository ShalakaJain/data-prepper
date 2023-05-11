/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SortingKeyOrderConfiguration {

    private static final String DEFAULT_SORT_ORDER = "asc";
    @JsonProperty("order")
    private String order = DEFAULT_SORT_ORDER;

    @JsonProperty("sort_key")
    private String sortKey;

    public String getSortKey() {
        return sortKey;
    }

    public String getOrder() {
            return order;
        }
    }