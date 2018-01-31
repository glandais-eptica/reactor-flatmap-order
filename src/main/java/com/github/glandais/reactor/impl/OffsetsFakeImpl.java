package com.github.glandais.reactor.impl;

import java.util.Collections;
import java.util.List;

import com.github.glandais.reactor.Message;
import com.github.glandais.reactor.Offsets;

public class OffsetsFakeImpl implements Offsets {

	@Override
	public Message check(Message receiverOffset) {
		// NOOP
		return null;
	}

	@Override
	public Long getNextStart() {
		// NOOP
		return -1L;
	}

	@Override
	public void checkAck(Message message) {
		// NOOP
	}

	@Override
	public List<Long> getBiggestIntervalsOffsets() {
		return Collections.emptyList();
	}

}
