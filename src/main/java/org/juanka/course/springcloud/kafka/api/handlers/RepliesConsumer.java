package org.juanka.course.springcloud.kafka.api.handlers;

import java.util.function.Consumer;

import org.juanka.course.springcloud.kafka.api.messaging.ReplyInbox;
import org.juanka.course.springcloud.kafka.api.models.dto.Reply;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;

/**
 * Esta clase se encarga de consumir los mensajes de respuesta que llegan a través de Kafka y completar
 * las solicitudes pendientes en el ReplyInbox. Cuando se recibe un mensaje, 
 * se extrae el correlationId de los encabezados del mensaje 
 * y se utiliza para completar la solicitud correspondiente en el ReplyInbox
 * con la carga útil del mensaje. Esto permite que las solicitudes asíncronas
 * puedan recibir sus respuestas de manera eficiente.
 */
@Configuration
public class RepliesConsumer {

	private final ReplyInbox replyinbox;
	
	public RepliesConsumer(ReplyInbox replyinbox) {
		this.replyinbox = replyinbox;
	}
	
	/** Bean que define un consumidor de mensajes de Kafka para manejar las respuestas.
	 * Este consumidor se activa cuando se recibe un mensaje de respuesta, 
	 * extrae el correlationId del encabezado del mensaje y completa la solicitud correspondiente
	 * en el ReplyInbox con la carga útil del mensaje.
	 */
	@Bean
	public Consumer<Message<Reply<?>>> handleReplies() {
		// Se devuelve una función lambda que actúa como consumidor de mensajes de Kafka
		return message -> {
			// Se extrae el correlationId de los encabezados del mensaje para identificar la solicitud correspondiente
			String correlationId = message.getHeaders().get("correlationId", String.class);
			// Se completa la solicitud correspondiente en el ReplyInbox utilizando el correlationId y la carga útil del mensaje
			replyinbox.complete(correlationId, message.getPayload());
		};
	}
	
}
