package org.juanka.course.springcloud.kafka.api.services;

import java.time.Duration;

import org.juanka.course.springcloud.kafka.api.models.dto.ProductDto;
import org.juanka.course.springcloud.kafka.api.models.dto.Reply;

/*
 * Interfaz que define el contrato para el servicio de comandos de productos, 
 * con un método para enviar un comando de creación de producto y esperar la respuesta
 */
public interface IProductCommandService {
	/**
	 * Método para enviar un comando de creación de producto al topic de Kafka y esperar la respuesta con un timeout
	 * @param dto DTO con los datos del producto a crear
	 * @param tiemout Duración máxima para esperar la respuesta del comando, si se supera el timeout se devuelve un error
	 * @return Reply con el resultado de la operación, si el comando se ejecuta correctamente se devuelve un Reply con 
	 * status SUCCESS y el cuerpo con los datos del producto creado, de lo contrario se devuelve un Reply con status ERROR y 
	 * el mensaje con el error ocurrido
	 */
	Reply<?> sendCreateAndAwait(ProductDto dto, Duration tiemout);
	
	Reply<?> sendReadAndAwait(Long id, Duration timeout);
	
}
