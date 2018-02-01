package com.github.glandais.reactor.impl;

import com.github.glandais.reactor.Message;
import com.github.glandais.reactor.Transform;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class TransformIO implements Transform {

	private long duration;

	public TransformIO(long duration) {
		super();
		this.duration = duration;
	}

	@Override
	public Message apply(Message input, Integer u) {
		try {
			Thread.sleep(duration);
		} catch (InterruptedException e) {
			// NOOP
		}
		return input;
	}

	@Override
	public Scheduler scheduler() {
		return Schedulers.elastic();
	}

	@Override
	public void stopScheduler() {
		Schedulers.elastic().dispose();
	}

}
