package org.flysoohigh.fileWatcher.beta;

import org.flysoohigh.fileWatcher.util.StreamHandler;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.*;

import static java.nio.file.StandardWatchEventKinds.*;

@Component
public class FolderWatcherRuntime {

    public void startWatching() {
        try {
            WatchService watcher = FileSystems.getDefault().newWatchService();

            Path logDir = Paths.get("/home/summer/dev/JAVA/FileWatcher/folder_input");
            logDir.register(watcher, ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE);

            while (true) {
                WatchKey key = watcher.take();
                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();

                    if (ENTRY_CREATE.equals(kind)) {
                        System.out.println("Entry was created on log dir.");

                        // TODO: 18.01.2020 Похоже, что не работает редирект из файла
//                        Process process = Runtime.getRuntime().exec("/home/summer/dev/KAFKA/kafka_2.12-2.3.1/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic input-topic < /home/summer/dev/JAVA/FileWatcher/folder_input/testData.txt");

                        // TODO: 18.01.2020 Works (!)
//                        Process process = Runtime.getRuntime().exec("/home/summer/dev/KAFKA/kafka_2.12-2.3.1/bin/kafka-topics.sh --zookeeper localhost:2181 --list");

                        // TODO: Просто прочитать из файла и самому записать в топик, через KafkaProducer ?
                        Process process = Runtime.getRuntime().exec("/home/summer/dev/KAFKA/kafka_2.12-2.3.1/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic input-topic");

                        StreamHandler errorHandler = new StreamHandler(process.getErrorStream(), "ERROR");
                        StreamHandler outputHandler = new StreamHandler(process.getInputStream(), "OUTPUT");

                        errorHandler.start();
                        outputHandler.start();

                        int exitVal = process.waitFor();
                        System.out.println("ExitValue: " + exitVal);

                    } else if (ENTRY_MODIFY.equals(kind)) {
                        System.out.println("Entry was modified on log dir.");
                    } else if (ENTRY_DELETE.equals(kind)) {
                        System.out.println("Entry was deleted from log dir.");
                    }
                }
                key.reset();
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
