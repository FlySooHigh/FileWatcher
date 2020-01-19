package org.flysoohigh.fileWatcher.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class TopicToFileWriter {

    @Autowired
    LinkedHashMap<String, Integer> fileStats;

    @Value("${kafka.output.topic}")
    private String kafkaOutputTopic;
    @Value("${kafka.group.id}")
    private String kafkaGroupId;
    @Value("${output.folder.path}")
    private String outputFolderPath;

    @KafkaListener(topics = "input-topic", groupId = "some_group_id")
    public void listen(String message) {
        System.out.println("Received message in group some_group_id: " + message);
        String lastFileName = String.valueOf(fileStats.keySet().toArray()[fileStats.size() - 1]);
        try {
            Files.write(Paths.get(outputFolderPath + "/" + lastFileName), message.getBytes(), StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
