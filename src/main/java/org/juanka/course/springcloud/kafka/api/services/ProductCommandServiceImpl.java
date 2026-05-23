package org.juanka.course.springcloud.kafka.api.services;

import org.juanka.course.springcloud.kafka.api.models.Command;
import org.juanka.course.springcloud.kafka.api.models.dto.ProductDto;
import org.springframework.cloud.stream.function.StreamBridge;

public class ProductCommandServiceImpl implements IProductCommandService {

	private final StreamBridge bridge;
	
	public ProductCommandServiceImpl(StreamBridge bridge) {
		this.bridge = bridge;
	}
	
	@Override
	public void sendCreate(ProductDto dto) {
		Command<ProductDto> cmd = new Command<ProductDto>("CREATE", null, dto);
		boolean hasBeenSent = this.bridge.send("commands-out-0", cmd);
		if(!hasBeenSent) {
			throw new IllegalStateException("No se pudo enviar el comando a Kafka");
		}
	}

}
