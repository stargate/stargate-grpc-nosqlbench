package io.nosqlbench.grpc.core;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class StubCacheTest {

    @Test
    public void shouldReturnHostsInARoundRobinFashion(){
        // given
        String hosts = "44.242.137.251,34.223.111.12";

        // when
        String host = StubCache.getHost(hosts);

        // then
        assertThat(host).isEqualTo("44.242.137.251");

        // when
        host = StubCache.getHost(hosts);

        // then
        assertThat(host).isEqualTo("34.223.111.12");

        // when
        host = StubCache.getHost(hosts);

        // then
        assertThat(host).isEqualTo("44.242.137.251");
    }

    @Test
    public void shouldReturnHostAsIsIfThereIsOnlyOneHost(){
        // given
        String hosts = "44.242.137.251";

        // when
        String host = StubCache.getHost(hosts);

        // then
        assertThat(host).isEqualTo("44.242.137.251");

        // when
        host = StubCache.getHost(hosts);

        // then
        assertThat(host).isEqualTo("44.242.137.251");
    }

}
