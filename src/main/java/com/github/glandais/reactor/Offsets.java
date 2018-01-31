package com.github.glandais.reactor;

import java.util.List;
import java.util.Map;

public interface Offsets {

	Message check(Message receiverOffset);

	Long getNextStart();

	void checkAck(Message message);

	Map<Long, Long> getBiggestIntervalsOffsets();

}