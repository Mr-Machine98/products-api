package org.juanka.course.springcloud.kafka.api.models;

import org.juanka.course.springcloud.kafka.api.models.enums.CommandType;

public record Command<T>(CommandType type, Long id, T body) {}
