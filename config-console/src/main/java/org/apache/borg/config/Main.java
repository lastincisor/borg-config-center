package org.apache.borg.config;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Util;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.watch.WatchEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        Args cmd = new Args();

        //JCommander.newBuilder().addObject(cmd).build().parse(args);

        CountDownLatch latch = new CountDownLatch(cmd.maxEvents);
        String node = "borg";
        ByteSequence key = ByteSequence.from(node, StandardCharsets.UTF_8);
        //Collection<URI> endpoints = Util.toURIs(cmd.endpoints);
        List<String> urls = new ArrayList<>();
        urls.add("localhost:2379");
        Collection<URI> endpoints = Util.toURIs(urls);
        Watch.Listener listener = Watch.listener(response -> {
            LOGGER.info("Watching for key={}", cmd.key);

            for (WatchEvent event : response.getEvents()) {
                LOGGER.info("type={}, key={}, value={}", event.getEventType(),
                        Optional.ofNullable(event.getKeyValue().getKey()).map(bs -> bs.toString(StandardCharsets.UTF_8)).orElse(""),
                        Optional.ofNullable(event.getKeyValue().getValue()).map(bs -> bs.toString(StandardCharsets.UTF_8))
                                .orElse(""));
            }

            latch.countDown();
        });

        try (Client client = Client.builder().endpoints(endpoints).build();
             Watch watch = client.getWatchClient();
             Watch.Watcher watcher = watch.watch(key, listener)) {

            latch.await();
        } catch (Exception e) {
            LOGGER.error("Watching Error {}", e);
            System.exit(1);
        }
    }

    public static class Args {
        @Parameter(required = true, names = { "-e", "--endpoints" }, description = "the etcd endpoints")
        private List<String> endpoints = new ArrayList<>();

        @Parameter(required = true, names = { "-k", "--key" }, description = "the key to watch")
        private String key;

        @Parameter(names = { "-m", "--max-events" }, description = "the maximum number of events to receive")
        private Integer maxEvents = Integer.MAX_VALUE;
    }
}
