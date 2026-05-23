package org.juanka.course.springcloud.kafka.api.controllers;

import java.util.Map;

import org.juanka.course.springcloud.kafka.api.models.dto.ProductDto;
import org.juanka.course.springcloud.kafka.api.services.IProductCommandService;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.validation.Valid;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;


@RestController
@RequestMapping("/products")
public class ProductController {

	private final IProductCommandService service;
	
	public ProductController(IProductCommandService service) {
		this.service = service;
	}
	
	@PostMapping
	public ResponseEntity<?> create(@Valid @RequestBody ProductDto dto) {
		this.service.sendCreate(dto);
		return ResponseEntity.ok().body(Map.of("message", "Success Sent"));
	}
	
}
