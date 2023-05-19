package org.opensearch.dataprepper.plugins.source.opensearch.configuration;

import java.io.Serializable;

public class SliceRequest implements Serializable {

    private SliceInformation slice;
    public static class SliceInformation implements  Serializable{
        private Long id;
        private Long max;

        public Long getMax() {
            return max;
        }

        public void setMax(Long max) {
            this.max = max;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }


    }

    public SliceInformation getSlice() {
        return slice;
    }

    public void setSlice(SliceInformation slice) {
        this.slice = slice;
    }
}
