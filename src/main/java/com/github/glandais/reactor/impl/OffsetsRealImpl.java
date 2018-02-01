package com.github.glandais.reactor.impl;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.glandais.reactor.Message;
import com.github.glandais.reactor.Offsets;

public class OffsetsRealImpl implements Offsets {

	private static final Logger LOGGER = LoggerFactory.getLogger(OffsetsRealImpl.class);

	private Long nextStart;

	private Map<Long, Message> positions = new TreeMap<>();

	private Map<Long, Long> countMissing = new TreeMap<>();

	public OffsetsRealImpl(long offset) {
		super();
		this.nextStart = offset;
		LOGGER.debug("Will start at _{}_", offset);
	}

	/* (non-Javadoc)
	 * @see com.github.glandais.reactor.Offsets#check(com.github.glandais.reactor.Message)
	 */
	@Override
	public synchronized Message check(Message receiverOffset) {
		// mark position
		this.positions.put(receiverOffset.offset(), receiverOffset);
		// continuous interval from start to end to ack
		Long end = null;
		Message checked = null;
		for (Entry<Long, Message> entry : this.positions.entrySet()) {
			Long pos = entry.getKey();
			// next pos is not adjacent               first pos is not start
			if ((checked != null && pos - end > 1) || (checked == null && !pos.equals(this.nextStart))) {
				break;
			}
			checked = entry.getValue();
			end = pos;
		}
		// found interval to ack
		if (checked != null) {
			// remove all marked positions
			for (long i = this.nextStart; i <= end; i++) {
				this.positions.remove(i);
			}
			// move forward
			this.nextStart = end + 1;
			LOGGER.debug("Marking _{}_ ack, acknowledging _{}_", receiverOffset.offset(), checked.offset());
		} else {
			logMissing(receiverOffset.offset(), this.nextStart);
		}
		return checked;
	}

	protected void logMissing(long offset, long start) {
		Long count = countMissing.getOrDefault(start, 0L);
		countMissing.put(start, count + 1);

		long diff = offset - start;
		LOGGER.debug("Marking _{}_ ack, missing _{}_ (-{} lag)", offset, start, diff);
	}

	/* (non-Javadoc)
	 * @see com.github.glandais.reactor.Offsets#getNextStart()
	 */
	@Override
	public Long getNextStart() {
		return nextStart;
	}

	/* (non-Javadoc)
	 * @see com.github.glandais.reactor.Offsets#checkAck(com.github.glandais.reactor.Message)
	 */
	@Override
	public void checkAck(Message message) {
		Message toAck = check(message);
		if (toAck != null) {
			LOGGER.debug("Ack {}", toAck);
		}
	}

	/* (non-Javadoc)
	 * @see com.github.glandais.reactor.Offsets#getBiggestIntervalsOffsets()
	 */
	@Override
	public Map<Long, Long> getBiggestIntervalsOffsets() {
		return countMissing.entrySet().stream().sorted((e1, e2) -> -e1.getValue().compareTo(e2.getValue())).limit(3)
				.collect(Collectors.toMap(Entry::getKey, Entry::getValue));
	}
}
