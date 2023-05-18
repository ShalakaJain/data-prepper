/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch;

import org.opensearch.client.json.JsonpDeserializer;
import org.opensearch.client.opensearch._types.ErrorResponse;
import org.opensearch.client.transport.Endpoint;
import org.opensearch.client.transport.endpoints.SimpleEndpoint;
import java.util.HashMap;
import java.util.Map;

public class ScrollRequest {

    private StringBuilder index;

    private Integer batchSize;

    private static final String GET_REQUEST_MEHTOD = "GET";

    private static final String SEARCH_URL = "/_search";

    static JsonpDeserializer<String> newResponseParser;

    final static JsonpDeserializer<Map> deserializer = new JacksonValueParser<>(Map.class);

    public ScrollRequest(ScrollBuilder scrollBuilder) {
    }

    public void setIndex(StringBuilder index) {
        this.index = index;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public static final Endpoint<ScrollRequest, Map, ErrorResponse> ENDPOINT =
            new SimpleEndpoint<>(
                    r -> GET_REQUEST_MEHTOD,
                    r -> "http://localhost:9200/" + r.index + SEARCH_URL,
                    r ->{
                        Map<String, String> params = new HashMap<>();
                        params.put("scroll", "10m");
                        return params;},
                    SimpleEndpoint.emptyMap(), false,
                    deserializer
            );
}

