package com.github.glandais.reactor;

import java.util.List;

public interface Offsets {

	Message check(Message receiverOffset);

	Long getNextStart();

	void checkAck(Message message);

	List<Long> getBiggestIntervalsOffsets();

}