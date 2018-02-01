package com.github.glandais.reactor.impl;

import com.github.glandais.reactor.Message;
import com.github.glandais.reactor.Trace;

public class TraceFakeImpl implements Trace {

	@Override
	public void hit(String where, Message message) {
		// NOOP
	}

	@Override
	public void printStats() {
		// NOOP
	}

	@Override
	public void print(Long key, Long lagCount) {
		// NOOP
	}

}
