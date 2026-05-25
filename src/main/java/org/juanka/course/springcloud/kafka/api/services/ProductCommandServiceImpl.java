package org.juanka.course.springcloud.kafka.api.services;

import org.juanka.course.springcloud.kafka.api.models.Command;
import org.juanka.course.springcloud.kafka.api.models.dto.ProductDto;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Service;

/**
 * Servicio encargado de enviar comandos relacionados
 * con productos hacia Kafka utilizando Spring Cloud Stream.
 *
 * <p>
 * Esta clase implementa la interfaz {@code IProductCommandService}
 * y actua como productor de mensajes.
 * </p>
 *
 * <p>
 * Su responsabilidad principal es construir comandos
 * y enviarlos al broker Kafka mediante StreamBridge.
 * </p>
 *
 * <p>
 * Ejemplo de flujo:
 * </p>
 *
 * <pre>
 * Cliente HTTP
 *      ↓
 * Controller
 *      ↓
 * ProductCommandServiceImpl
 *      ↓
 * Kafka Topic
 *      ↓
 * Consumer
 * </pre>
 *
 * <p>
 * Esta arquitectura permite desacoplar servicios
 * utilizando comunicacion asincrona basada en eventos.
 * </p>
 *
 * @author Juan
 * @version 1.0
 */
@Service
public class ProductCommandServiceImpl implements IProductCommandService {

	// StreamBridge permite enviar mensajes dinamicamente
	// hacia canales definidos en Spring Cloud Stream.
	//
	// Funciona como un puente entre la aplicacion
	// y el broker de mensajeria (Kafka).
	private final StreamBridge bridge;
	
	
	/**
	 * Constructor con inyeccion de dependencias.
	 *
	 * <p>
	 * Spring automaticamente inyecta una instancia
	 * de StreamBridge.
	 * </p>
	 *
	 * @param bridge componente utilizado para enviar mensajes a Kafka
	 */
	public ProductCommandServiceImpl(StreamBridge bridge) {
		
		// Asigna la instancia recibida a la variable de clase.
		this.bridge = bridge;
	}
	
	
	/**
	 * Envia un comando CREATE hacia Kafka
	 * para crear un producto.
	 *
	 * <p>
	 * El metodo construye un objeto Command<ProductDto>
	 * y lo publica en el canal:
	 * </p>
	 *
	 * <pre>
	 * commands-out-0
	 * </pre>
	 *
	 * <p>
	 * Finalmente el mensaje llega al topic Kafka:
	 * </p>
	 *
	 * <pre>
	 * products.commands
	 * </pre>
	 *
	 * @param dto informacion del producto a crear
	 */
	@Override
	public void sendCreate(ProductDto dto) {
		
		// Construye un comando de tipo CREATE.
		//
		// Estructura:
		//
		// type -> tipo de operacion
		// id   -> identificador (null porque aun no existe)
		// body -> datos del producto
		Command<ProductDto> cmd = new Command<ProductDto>("CREATE", null, dto);
		
		
		// Envia el comando al canal:
		//
		// commands-out-0
		//
		// Spring Cloud Stream automaticamente
		// lo enviara al topic Kafka configurado.
		//
		// El metodo retorna:
		//
		// true  -> mensaje enviado correctamente
		// false -> fallo en el envio
		boolean hasBeenSent = this.bridge.send("commands-out-0", cmd);
		
		
		// Valida si el mensaje NO pudo enviarse.
		if(!hasBeenSent) {
			
			// Lanza una excepcion indicando
			// que el envio fallo.
			throw new IllegalStateException("No se pudo enviar el comando a Kafka");
		}
	}

}
