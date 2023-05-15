/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch;

import org.opensearch.client.util.ObjectBuilder;
import static java.util.Objects.requireNonNull;
public class ScrollBuilder implements ObjectBuilder<ScrollRequest> {
   private StringBuilder index;
    private String scrollTime;
    private String batchSize;
    public final ScrollBuilder size(final StringBuilder index) {
        this.index = index;
        return this;
    }
    public final ScrollBuilder size(final String scrollTime) {
        this.scrollTime = scrollTime;
        return this;
    }
    public final ScrollBuilder keep_alive( final String batchSize ) {
        this.batchSize = batchSize;
        return this;
    }
    @Override
    public ScrollRequest build() {
        requireNonNull(this.scrollTime, "scroll time was not set");
        return new ScrollRequest(this);
    }
}
