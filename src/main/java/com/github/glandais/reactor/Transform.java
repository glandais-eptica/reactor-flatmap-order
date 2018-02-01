package com.github.glandais.reactor;

import java.util.function.BiFunction;

import reactor.core.scheduler.Scheduler;

public interface Transform extends BiFunction<Message, Integer, Message> {

	Scheduler scheduler();

	void stopScheduler();

}
