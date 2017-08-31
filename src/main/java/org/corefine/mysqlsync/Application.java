package org.corefine.mysqlsync;

import org.corefine.mysqlsync.service.SyncService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@SpringBootApplication
@EnableScheduling
public class Application  {
	@Autowired
	private SyncService service;
	
	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}
	
	@Scheduled(cron = "${syncJobCron}")
	public void jobSync() {
		service.sync();
	}
}
