package com.github.glandais.reactor.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.glandais.reactor.Message;
import com.github.glandais.reactor.Transform;
import com.google.common.hash.Hashing;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class TransformCPU implements Transform {

	private static final Logger LOGGER = LoggerFactory.getLogger(TransformCPU.class);

	private static final Scheduler SCHEDULER = Schedulers.newParallel("process", 32);

	private long cycles;

	public TransformCPU(long cycles) {
		super();
		this.cycles = cycles;
	}

	@Override
	public Message apply(Message input, Integer step) {
		long out = System.nanoTime();
		int n = 0;
		do {
			out = Hashing.sha512().hashLong(out).asLong();
		} while (n++ < this.cycles);
		LOGGER.debug("{} transformed {} {}", input, step, n);
		return input;
	}

	@Override
	public Scheduler scheduler() {
		return SCHEDULER;
	}

	@Override
	public void stopScheduler() {
		SCHEDULER.dispose();
	}

}
