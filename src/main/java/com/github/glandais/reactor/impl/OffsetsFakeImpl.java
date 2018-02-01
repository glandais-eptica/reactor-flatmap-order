package com.github.glandais.reactor.impl;

import java.util.Collections;
import java.util.List;

import org.jooq.lambda.tuple.Tuple2;

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
	public List<Tuple2<Long, Long>> getBiggestIntervalsOffsets() {
		return Collections.emptyList();
	}

}
