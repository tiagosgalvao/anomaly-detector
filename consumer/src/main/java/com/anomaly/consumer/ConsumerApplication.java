package com.anomaly.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(ConsumerProperties.class)
public class ConsumerApplication {

	static void main(String[] args) {
		SpringApplication.run(ConsumerApplication.class, args);
	}

}
