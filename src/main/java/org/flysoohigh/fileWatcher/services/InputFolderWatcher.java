package org.flysoohigh.fileWatcher.services;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;

@Component
public class InputFolderWatcher {

    Logger logger = LoggerFactory.getLogger(InputFolderWatcher.class);

    @Autowired
    private KafkaProducer<String, String> producer;
    @Autowired
    Map<String, Integer> fileStats;

    @Value("${input.folder.path}")
    private String inputFolderPath;
    @Value("${processed.folder.path}")
    private String processedFolderPath;
    @Value("${kafka.input.topic}")
    private String kafkaInputTopic;

    public void startWatching() {
        try {
            processExistingFiles();

            WatchService watcher = FileSystems.getDefault().newWatchService();
            Path inputFolder = Paths.get(inputFolderPath);
            inputFolder.register(watcher, ENTRY_CREATE);

            while (true) {
                WatchKey key = watcher.take();
                for (WatchEvent<?> event : key.pollEvents()) {
                    String fileName = event.context().toString();
                    WatchEvent.Kind<?> kind = event.kind();

                    if (ENTRY_CREATE.equals(kind)) {
                        processFile(inputFolder, fileName);
                    }
                }
                key.reset();
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void processExistingFiles() throws IOException {
        try (Stream<Path> paths = Files.walk(Paths.get(inputFolderPath))) {
            paths.filter(Files::isRegularFile)
                    .forEach(file -> {
                        try {
                            processFile(file.getParent(), file.getFileName().toString());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
        }
    }

    private void processFile(Path inputFolder, String fileName) throws IOException {
        logger.info("Файл {} поступил на обработку...", fileName);

        final int[] counter = {0};

        try (Stream<String> stream = Files.lines(Paths.get(inputFolder + "/" + fileName))) {
            stream.forEach(line -> {
                counter[0]++;
                producer.send(new ProducerRecord<>(kafkaInputTopic, line));
            });
        }

        fileStats.put(fileName, counter[0]);

        Files.move(Paths.get(inputFolder + "/" + fileName),
                Paths.get(processedFolderPath + "/" + fileName),
                StandardCopyOption.REPLACE_EXISTING);

        logger.info("Файл {} был обработан и перенесен в {} директорию", fileName, processedFolderPath);

//        Files.deleteIfExists(Paths.get(inputFolder + "/" + fileName));
//        logger.info("Файл {} был обработан и удален из {} директории", fileName, inputFolderPath);
    }
}
