package org.juanka.course.springcloud.kafka.api.controllers;

import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.GetMapping;


@RestController
public class PingController {

	@GetMapping("/ping")
	public String ping() {
		return "Ping OK, it works.";
	}
	
}
