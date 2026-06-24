package org.juanka.course.springcloud.kafka.api.models.dto;

import org.juanka.course.springcloud.kafka.api.models.enums.ReplyStatus;

public record Reply<T>(ReplyStatus status, String message, T body) {

}
