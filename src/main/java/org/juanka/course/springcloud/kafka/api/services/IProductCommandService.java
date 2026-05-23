package org.juanka.course.springcloud.kafka.api.services;

import org.juanka.course.springcloud.kafka.api.models.dto.ProductDto;

public interface IProductCommandService {
	void sendCreate(ProductDto dto);
}
