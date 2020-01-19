package org.flysoohigh.fileWatcher;

import org.flysoohigh.fileWatcher.services.InputFolderWatcher;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;

@SpringBootApplication
public class FileWatcherApplication {

	public static void main(String[] args) {
		SpringApplication application = new SpringApplication(FileWatcherApplication.class);
		addInitHooks(application);
		application.run(args);
	}

	private static void addInitHooks(SpringApplication application) {
		application.addListeners((ApplicationListener<ApplicationReadyEvent>) event -> {
			InputFolderWatcher inputFolderWatcher = (InputFolderWatcher) event.getApplicationContext().getBean("inputFolderWatcher");
			inputFolderWatcher.startWatching();
		});
	}

}
