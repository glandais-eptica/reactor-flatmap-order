package com.github.glandais.reactor.impl;

import com.github.glandais.reactor.Message;
import com.github.glandais.reactor.Transform;

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

}
