/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch;

import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.TimerTask;

public class OpenSearchScrollTask extends TimerTask {

    private static final Logger LOG = LoggerFactory.getLogger(SourceInfoProvider.class);

    OpenSearchSourceConfiguration openSearchSourceConfiguration = null;

    OpenSearchClient osClient=null;

    Buffer<Record<Event>> buffer = null;

    public OpenSearchScrollTask(OpenSearchSourceConfiguration openSearchSourceConfiguration, Buffer<Record<Event>> buffer) {
        this.openSearchSourceConfiguration = openSearchSourceConfiguration;
        this.buffer = buffer;
    }

    @Override
    public void run() {
        int numRuns = 0;
        while (numRuns++ <= openSearchSourceConfiguration.getSchedulingParameterConfiguration().getJobCount()) {
            OpenSearchApiCalls openSearchApiCalls = new OpenSearchApiCalls(osClient);
            openSearchApiCalls.getScrollResponse(openSearchSourceConfiguration , buffer);
        }
    }
}
