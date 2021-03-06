package com.github.glandais.reactor.impl;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.jooq.lambda.Seq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.glandais.reactor.Markers;
import com.github.glandais.reactor.Message;
import com.github.glandais.reactor.Trace;
import com.google.common.math.StatsAccumulator;

public class TraceRealImpl implements Trace {

	private static final Logger LOGGER = LoggerFactory.getLogger(TraceRealImpl.class);

	private List<String> labels = new ArrayList<>();

	private Map<Long, List<Instant>> events = new HashMap<>();

	private Map<Long, List<Long>> positions = new HashMap<>();

	private Map<Long, List<String>> threads = new HashMap<>();

	private Map<Integer, StatsAccumulator> stats = new TreeMap<>();

	private Map<Integer, AtomicLong> seen = new TreeMap<>();

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
		list.add(now);

		int listSize = list.size();
		if (labels.size() < listSize) {
			labels.add(where);
		}

		long position = seen.computeIfAbsent(listSize - 1, i -> new AtomicLong(0L)).getAndIncrement();
		positions.computeIfAbsent(offset, i -> new ArrayList<>()).add(position);
		threads.computeIfAbsent(offset, i -> new ArrayList<>()).add(Thread.currentThread().getName());

		// store duration for offset
		if (listSize > 1) {
			int event = listSize - 1;
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
		positions.keySet().forEach(offset -> LOGGER.debug(Markers.STATS, "{} : {}", offset,
				Seq.zip(positions.get(offset), threads.get(offset))));
	}

	protected void printStats(Integer k, StatsAccumulator v) {
		String label = labels.get(k - 1) + " -> " + labels.get(k);
		LOGGER.debug(Markers.STATS, "Stats of {}", label);
		LOGGER.debug(Markers.STATS, "  {} count : {}", label, v.count());
		LOGGER.debug(Markers.STATS, "  {} min : {}", label, v.min());
		LOGGER.debug(Markers.STATS, "  {} max : {}", label, v.max());
		LOGGER.debug(Markers.STATS, "  {} mean : {}", label, v.mean());
		LOGGER.debug(Markers.STATS, "  {} populationStandardDeviation : {}", label, v.populationStandardDeviation());
		LOGGER.debug(Markers.STATS, "  {} sampleStandardDeviation : {}", label, v.sampleStandardDeviation());
	}

	/* (non-Javadoc)
	 * @see com.github.glandais.reactor.Trace#print(java.lang.Long)
	 */
	@Override
	public void print(Long key, Long lagCount) {
		LOGGER.info(Markers.LAGS, "{} ({} lags)", key, lagCount);
		List<Instant> value = events.get(key);
		LOGGER.info(Markers.LAGS, "History for _{}_", key);
		List<Long> pos = positions.get(key);
		List<String> thr = threads.get(key);
		LOGGER.info(Markers.LAGS, "Position/thread history : {}", Seq.zip(pos, thr));
		LOGGER.info(Markers.LAGS, "Start (pos:{}, thread:{}) : {}", pos.get(0), thr.get(0), value.get(0));
		for (int i = 1; i < value.size(); i++) {
			LOGGER.info(Markers.LAGS, "Next  (pos:{}, thread:{}) : {} ({} -> {}) ", pos.get(i), thr.get(i),
					Duration.between(value.get(i - 1), value.get(i)), labels.get(i - 1), labels.get(i));
		}
		LOGGER.info(Markers.LAGS, "Total  : {}", Duration.between(value.get(0), value.get(value.size() - 1)));
	}

}
