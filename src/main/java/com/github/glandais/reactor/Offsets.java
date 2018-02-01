package com.github.glandais.reactor;

import java.util.List;

import org.jooq.lambda.tuple.Tuple2;

public interface Offsets {

	Message check(Message receiverOffset);

	Long getNextStart();

	void checkAck(Message message);

	List<Tuple2<Long, Long>> getBiggestIntervalsOffsets();

}