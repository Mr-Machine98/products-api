package org.juanka.course.springcloud.kafka.api.messaging;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.juanka.course.springcloud.kafka.api.models.dto.Reply;
import org.springframework.stereotype.Component;

/**
 * Clase que actúa como un buzón de entrada para las respuestas de Kafka.
 * Permite registrar futuros de CompletableFuture asociados a un correlationId y
 * completar esos futuros cuando se recibe una respuesta con el mismo correlationId.
 * Utiliza un ConcurrentHashMap para almacenar los futuros pendientes de completar,
 * lo que garantiza la seguridad en entornos concurrentes. 
 * Esta clase es fundamental para implementar un patrón de
 * solicitud-respuesta en Kafka, donde el cliente envía un
 * comando y espera una respuesta correlacionada.
*/
@Component
public class ReplyInbox {
	
	// Mapa concurrente para almacenar los futuros pendientes de completar, asociados a un correlationId
	private final ConcurrentHashMap<String, CompletableFuture<Reply<?>>> pending = new ConcurrentHashMap<>();
	
	/**
	 * Registra un nuevo futuro para esperar una respuesta correlacionada.
	 * @param correlationId El ID de correlación para el futuro.
	 * @return El futuro registrado.
	 */
	public CompletableFuture<Reply<?>> register(String correlationId) {
		// Se crea un nuevo futuro y se asocia al correlationId en el mapa de pendientes
		CompletableFuture<Reply<?>> future = new CompletableFuture<>();
		// Se agrega el futuro al mapa de pendientes con el correlationId como clave
		pending.put(correlationId, future);
		// Se devuelve el futuro registrado para que el cliente pueda esperar su resultado
		return future;
	}
	
	/**
	 * Completa el futuro asociado al correlationId con la respuesta recibida.
	 * @param correlationId El ID de correlación para identificar el futuro a completar.
	 * @param reply La respuesta que se utilizará para completar el futuro.
	 */
	public void complete(String correlationId, Reply<?> reply) {
		// Se verifica que el correlationId no sea nulo para evitar errores de NullPointerException
		if (correlationId == null) {
			throw new NullPointerException("correlationId cannot be null");
		}
		// Se intenta remover el futuro asociado al correlationId del mapa de pendientes
		CompletableFuture<Reply<?>> future = pending.remove(correlationId);
		// Si se encuentra un futuro asociado al correlationId, se completa con la respuesta recibida
		if (future != null) {
			future.complete(reply);
		}
	}
	
}
