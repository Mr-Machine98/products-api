package org.juanka.course.springcloud.kafka.api.services;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.juanka.course.springcloud.kafka.api.messaging.ReplyInbox;
import org.juanka.course.springcloud.kafka.api.models.Command;
import org.juanka.course.springcloud.kafka.api.models.dto.ProductDto;
import org.juanka.course.springcloud.kafka.api.models.dto.Reply;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Service
public class ProductCommandServiceImpl implements IProductCommandService {

	// Inyeccion de la dependencia StreamBridge para enviar mensajes a kafka
	private final StreamBridge bridge;

    // Logger para registrar mensajes de log
	private static final Logger logger = LoggerFactory.getLogger(ProductCommandServiceImpl.class);

	// Inyeccion de la dependencia ReplyInbox para registrar y esperar respuestas de kafka
	private final ReplyInbox replyInbox;
	
	public ProductCommandServiceImpl(StreamBridge bridge, ReplyInbox replyInbox) {
		this.bridge = bridge;
		this.replyInbox = replyInbox;
	}
	
	@Override
	public Reply<?> sendCreateAndAwait(ProductDto dto, Duration timeout) {
		
        // Se crea un comando con la acción "CREATE", sin id y con el DTO como payload
		Command<ProductDto> cmd = new Command<ProductDto>("CREATE", null, dto);
		
		// Se genera un correlationId único para correlacionar la respuesta con el comando enviado
		String correlationId = UUID.randomUUID().toString();
		
		logger.info("Api Products Client Creating product with correlationId: {}", correlationId);
		
		// Se registra el correlationId en el ReplyInbox para esperar la respuesta correspondiente
		CompletableFuture<Reply<?>> future = this.replyInbox.register(correlationId);
		
		// Se construye el mensaje con el comando como payload y el correlationId en los headers
		Message<Command<ProductDto>> msg = MessageBuilder
				.withPayload(cmd).setHeader("correlationId", correlationId).build();
		
		// Se envía el mensaje al topic "commands-out-0" utilizando el StreamBridge y se verifica si el envío fue exitoso
		boolean hasBeenSent = this.bridge.send("commands-out-0", msg);
		
		// Si el mensaje no se pudo enviar, se lanza una excepción indicando que no se pudo enviar el comando a Kafka
		if(!hasBeenSent) {
			throw new IllegalStateException("No se pudo enviar el comando a Kafka");
		}
		
		try {
			// Se espera la respuesta del comando utilizando el correlationId registrado en el ReplyInbox, con un timeout especificado
			return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			e.printStackTrace();
			throw new RuntimeException("Error al esperar la respuesta del comando products-commands desde kafka.");
		}
	}

}
