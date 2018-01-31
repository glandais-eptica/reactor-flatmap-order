package com.github.glandais.reactor.impl;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.glandais.reactor.Message;
import com.github.glandais.reactor.Trace;
import com.google.common.math.StatsAccumulator;

public class TraceRealImpl implements Trace {

	private static final Logger LOGGER = LoggerFactory.getLogger(TraceRealImpl.class);

	private List<String> labels = new ArrayList<>();

	private Map<Long, List<Instant>> events = new HashMap<>();

	private Map<Integer, StatsAccumulator> stats = new HashMap<>();

	/* (non-Javadoc)
	 * @see com.github.glandais.reactor.Trace#hit(java.lang.String, com.github.glandais.reactor.Message)
	 */
	@Override
	public synchronized void hit(String where, Message message) {
		Instant now = Instant.now();

		LOGGER.debug("{} {}", where, message);

		Long offset = message.offset();
		// store event for offset
		List<Instant> list = events.computeIfAbsent(offset, i -> new ArrayList<>());
		if (list.isEmpty()) {
			events.put(offset, list);
		}
		list.add(now);

		if (labels.size() < list.size()) {
			labels.add(where);
		}

		// store duration for offset
		if (list.size() > 1) {
			int event = list.size() - 1;
			StatsAccumulator statsAccu = stats.computeIfAbsent(event, a -> new StatsAccumulator());
			statsAccu.add(Duration.between(list.get(event - 1), list.get(event)).toMillis());
		}
	}

	/* (non-Javadoc)
	 * @see com.github.glandais.reactor.Trace#printStats()
	 */
	@Override
	public void printStats() {
		stats.forEach(this::printStats);
	}

	protected void printStats(Integer k, StatsAccumulator v) {
		String label = labels.get(k);
		LOGGER.info("Stats of level {}", label);
		LOGGER.info("  {} count : {}", label, v.count());
		LOGGER.info("  {} min : {}", label, v.min());
		LOGGER.info("  {} max : {}", label, v.max());
		LOGGER.info("  {} mean : {}", label, v.mean());
		LOGGER.info("  {} populationStandardDeviation : {}", label, v.populationStandardDeviation());
		LOGGER.info("  {} sampleStandardDeviation : {}", label, v.sampleStandardDeviation());
	}

	/* (non-Javadoc)
	 * @see com.github.glandais.reactor.Trace#print(java.lang.Long)
	 */
	@Override
	public void print(Long key) {
		List<Instant> value = events.get(key);
		LOGGER.info("History for _{}_", key);
		LOGGER.info("Start : {}", value.get(0));
		for (int i = 1; i < value.size(); i++) {
			LOGGER.info("Next  : {} ({} -> {}) ", Duration.between(value.get(i - 1), value.get(i)), labels.get(i - 1),
					labels.get(i));
		}
		LOGGER.info("Total  : {}", Duration.between(value.get(0), value.get(value.size() - 1)));
	}

}
