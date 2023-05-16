package org.opensearch.dataprepper.plugins.source.opensearch;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.io.IOException;

public interface SearchAPICalls {
      void generatePitId(final OpenSearchSourceConfiguration openSearchSourceConfiguration ,Buffer<Record<Event>> buffer) throws IOException;

      String searchPitIndexes( final String pitID ,final OpenSearchSourceConfiguration openSearchSourceConfiguration, Buffer<Record<Event>> buffer);

      void generateScrollId(final OpenSearchSourceConfiguration openSearchSourceConfiguration ,Buffer<Record<Event>> buffers);

      String searchScrollIndexes(final OpenSearchSourceConfiguration openSearchSourceConfiguration);

}