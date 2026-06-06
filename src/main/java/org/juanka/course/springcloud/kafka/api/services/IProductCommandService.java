package org.juanka.course.springcloud.kafka.api.services;

import java.time.Duration;

import org.juanka.course.springcloud.kafka.api.models.dto.ProductDto;
import org.juanka.course.springcloud.kafka.api.models.dto.Reply;

/*
 * Interfaz que define el contrato para el servicio de comandos de productos, 
 * con un método para enviar un comando de creación de producto y esperar la respuesta
 */
public interface IProductCommandService {
	Reply<?> sendCreateAndAwait(ProductDto dto, Duration tiemout);
}
