package org.opensearch.dataprepper.plugins.source.opensearch;
import org.opensearch.client.opensearch.OpenSearchClient;

import java.io.IOException;

public interface SearchAPICalls {
      void generatePitId(final OpenSearchSourceConfiguration openSearchSourceConfiguration) throws IOException;

      String searchPitIndexes(final String pitId, OpenSearchSourceConfiguration openSearchSourceConfiguration);

      String generateScrollId(final OpenSearchSourceConfiguration openSearchSourceConfiguration);

      String searchScrollIndexes(final OpenSearchSourceConfiguration openSearchSourceConfiguration);

}