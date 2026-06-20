package org.juanka.course.springcloud.kafka.api.controllers;

import java.time.Duration;
import java.util.Map;

import org.juanka.course.springcloud.kafka.api.models.dto.ProductDto;
import org.juanka.course.springcloud.kafka.api.models.dto.Reply;
import org.juanka.course.springcloud.kafka.api.services.IProductCommandService;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import jakarta.validation.Valid;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.PutMapping;

@RestController
@RequestMapping("/products")
public class ProductController {

	private final IProductCommandService service;
	
	public ProductController(IProductCommandService service) {
		this.service = service;
	}
	
	/*
	 * Método para crear un producto, recibe un DTO con los datos del producto a crear
	 * y devuelve una respuesta con el resultado de la operación
	 */
	@PostMapping
	public ResponseEntity<?> create(@Valid @RequestBody ProductDto dto) {
		// Se envía el mensaje al topic y se espera la respuesta con un timeout de 5 segundos
		Reply<?> reply = this.service.sendCreateAndAwait(dto, Duration.ofSeconds(5));
		return getResponseEntity(reply);
	}
	
	/*
	 * Método para obtener un producto por su id, recibe el id del producto como parámetro
	 * y devuelve una respuesta con el resultado de la operación
	 */
	@GetMapping("/{id}")
	public ResponseEntity<?> getByid(@PathVariable Long id) {
		Reply<?> reply = this.service.sendReadAndAwait(id, Duration.ofSeconds(5));
		return getResponseEntity(reply);
	}
	
	/*
	 * Método para obtener todos los productos, no recibe parámetros y devuelve una respuesta con el resultado de la operación
	 */
	@GetMapping
	public ResponseEntity<?> getAll() {
		Reply<?> reply = this.service.sendReadAllAndAwait(Duration.ofSeconds(5));
		return getResponseEntity(reply);
	}
	
	/*
	 * Método para actualizar un producto, recibe el id del producto a actualizar como parámetro y un DTO con los datos a actualizar
	 * y devuelve una respuesta con el resultado de la operación
	 */
	@PutMapping("/{id}")
	public ResponseEntity<?> update(@PathVariable Long id, @Valid @RequestBody ProductDto dto) {
		Reply<?> reply = this.service.sendUpdateAndAwait(dto, id, Duration.ofSeconds(5));
		return getResponseEntity(reply);
	}
	
	/*
	 * Método para obtener una respuesta HTTP a partir de un Reply, si el estado del Reply es SUCCESS se devuelve el
	 *  cuerpo de la respuesta
	 * con un status 200 OK, de lo contrario se devuelve un error con el mensaje del Reply y un status 400 Bad Request
	 */
	private ResponseEntity<?> getResponseEntity(Reply<?> reply) {
		// Se verifica el estado de la respuesta si es SUCCESS 
		// se devuelve el cuerpo de la respuesta, de lo contratio se devuelve un error con el mensaje de la respuesta
		if("SUCCESS".equalsIgnoreCase(reply.status())) {
			return ResponseEntity.ok(reply.body());
		}
		// Si el estado no es SUCCESS, se devuelve un error con el mensaje de la respuesta
		return ResponseEntity.badRequest().body(Map.of("error", reply.message()));
	}
	
	
}
