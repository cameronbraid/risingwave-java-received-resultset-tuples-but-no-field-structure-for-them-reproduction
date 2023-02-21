package risingwave;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;

import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import lombok.extern.slf4j.Slf4j;

@SuppressWarnings(value = { "rawtypes", "unchecked" })
@Slf4j
public class TestCase {

    @Test
    public void testWhitespaceBeforeSelect() throws Exception {

        try (var c = new GenericContainer("risingwavelabs/risingwave:latest")) {

            c.withStartupTimeout(Duration.ofSeconds(60))
                    .withNetwork(Network.SHARED)
                    .withNetworkAliases("risingwave")
                    .withStartupTimeout(Duration.ofSeconds(30))
                    .withCommand("playground")
                    .withLogConsumer(new Slf4jLogConsumer(log).withPrefix("risingwave"))
                    .withExposedPorts(4566)
                    .waitingFor(Wait.forLogMessage(".*Server Listening at 0.0.0.0:5688.*", 1));

            c.start();

            var jdbi = Jdbi.create(String.format("jdbc:postgresql://localhost:%s/dev", c.getMappedPort(4566)), "root",
                    "root");
            try (var handle = jdbi.open()) {
                var list = handle.createQuery(" select 1").mapToMap().list();
                assertEquals(1, list.size());
            }

        }
    }
    @Test
    public void testNoWhitespace() throws Exception {

        try (var c = new GenericContainer("risingwavelabs/risingwave:latest")) {

            c.withStartupTimeout(Duration.ofSeconds(60))
                    .withNetwork(Network.SHARED)
                    .withNetworkAliases("risingwave")
                    .withStartupTimeout(Duration.ofSeconds(30))
                    .withCommand("playground")
                    .withLogConsumer(new Slf4jLogConsumer(log).withPrefix("risingwave"))
                    .withExposedPorts(4566)
                    .waitingFor(Wait.forLogMessage(".*Server Listening at 0.0.0.0:5688.*", 1));

            c.start();

            var jdbi = Jdbi.create(String.format("jdbc:postgresql://localhost:%s/dev", c.getMappedPort(4566)), "root",
                    "root");
            try (var handle = jdbi.open()) {
                var list = handle.createQuery("select 1").mapToMap().list();
                assertEquals(1, list.size());
            }

        }
    }
}
