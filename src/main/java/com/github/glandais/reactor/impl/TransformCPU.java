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

	private long duration;

	public TransformCPU(long duration) {
		super();
		this.duration = duration;
	}

	@Override
	public Message apply(Message input, Integer step) {
		long now = System.currentTimeMillis();
		long end = now + duration;
		long out = now;
		int n = 0;
		do {
			out = Hashing.sha512().hashLong(out).asLong();
			n++;
		} while (System.currentTimeMillis() <= end);
		LOGGER.debug("{} transformed {} {}", input, step, n);
		return input;
	}

	@Override
	public Scheduler scheduler() {
		return SCHEDULER;
	}

}
