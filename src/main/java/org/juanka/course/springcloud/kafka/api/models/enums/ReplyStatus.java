package org.juanka.course.springcloud.kafka.api.models.enums;

public enum ReplyStatus {
	SUCCESS,
	ERROR;
	
	public boolean isSuccess() {
		return this == SUCCESS;
	}
}
