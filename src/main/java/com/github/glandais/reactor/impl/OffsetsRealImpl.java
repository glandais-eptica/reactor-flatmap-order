package com.github.glandais.reactor.impl;

import java.util.List;
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
		this.positions.put(offset, null);
		this.nextStart = offset;
		LOGGER.debug("Will start at _{}_", offset);
	}

	/* (non-Javadoc)
	 * @see com.github.glandais.reactor.Offsets#check(com.github.glandais.reactor.Message)
	 */
	@Override
	public synchronized Message check(Message receiverOffset) {
		this.positions.put(receiverOffset.offset(), receiverOffset);
		Long start = null;
		Long end = null;
		Message checked = null;
		for (Entry<Long, Message> entry : this.positions.entrySet()) {
			if (entry.getValue() == null) {
				logMissing(receiverOffset.offset(), entry.getKey());
				return null;
			}
			if (end != null && entry.getKey() - end > 1) {
				break;
			} else {
				Long pos = entry.getKey();
				if (start == null) {
					if (!pos.equals(this.nextStart)) {
						break;
					}
					start = pos;
				}
				end = pos;
				checked = entry.getValue();
			}
		}
		if (start != null) {
			for (long i = start; i <= end; i++) {
				this.positions.remove(i);
			}
			this.nextStart = end + 1;
		}
		if (checked != null) {
			LOGGER.debug("Marking _{}_ ack, acknowledging _{}_", receiverOffset.offset(), checked.offset());
		} else {
			logMissing(receiverOffset.offset(), this.nextStart);
		}
		return checked;
	}

	/* (non-Javadoc)
	 * @see com.github.glandais.reactor.Offsets#getNextStart()
	 */
	@Override
	public Long getNextStart() {
		return nextStart;
	}

	protected void logMissing(long offset, long start) {
		Long count = countMissing.getOrDefault(start, 0L);
		countMissing.put(start, count + 1);

		long diff = offset - start;
		LOGGER.debug("Marking _{}_ ack, missing _{}_ (-{} lag)", offset, start, diff);
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
	public List<Long> getBiggestIntervalsOffsets() {
		return countMissing.entrySet().stream().sorted((e1, e2) -> -e1.getValue().compareTo(e2.getValue())).limit(3)
				.map(Entry::getKey).collect(Collectors.toList());
	}
}
