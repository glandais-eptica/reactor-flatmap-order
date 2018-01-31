package com.github.glandais.reactor;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.logging.Level;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.glandais.reactor.impl.OffsetsFakeImpl;
import com.github.glandais.reactor.impl.TraceFakeImpl;
import com.google.common.hash.Hashing;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.function.Function3;

public class Main {

	private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

	// Number of items
	public static final int COUNT = 10000;

	// number of mapping
	public static final int FLATMAP_COUNT = 1;

	public static final Level LEVEL = Level.FINE;

	public static final Scheduler SCHEDULER_PARALLEL = Schedulers.newParallel("sched");

	private static final long TRANSFORM_MIN_DURATION = 2;

	public static void main(String[] args) {
		// testing various flatMap alikes

		bench("flatMap", Main::flatMap);
		//		bench("concatMap", Main::concatMap);
		bench("flatMapSequential", Main::flatMapSequential);

		SCHEDULER_PARALLEL.dispose();
	}

	public static Flux<Message> flatMap(Flux<Message> flux, Trace trace, Integer step) {
		return flux.flatMap(m -> transform(m, trace, step));
	}

	//	public static Flux<Message> concatMap(Flux<Message> flux, Trace trace, Integer step) {
	//		return flux.concatMap(m -> transform(m, trace, step));
	//	}

	public static Flux<Message> flatMapSequential(Flux<Message> flux, Trace trace, Integer step) {
		return flux.flatMapSequential(m -> transform(m, trace, step));
	}

	private static void bench(String name, Function3<Flux<Message>, Trace, Integer, Flux<Message>> mapper) {
		LOGGER.info("****************** {} ******************", name);

		// some context
		Instant start = Instant.now();
		//		Offsets offsets = new OffsetsRealImpl(0);
		//		Trace trace = new TraceRealImpl();
		Offsets offsets = new OffsetsFakeImpl();
		Trace trace = new TraceFakeImpl();

		// generate a list of message
		Flux<Message> flux = Flux.range(0, COUNT).map(i -> new Message((long) i, offsets))
				.doOnNext(m -> trace.hit("sourced", m));

		// mapping flux using provided mapper
		for (int j = 0; j < FLATMAP_COUNT; j++) {
			int step = j;
			flux = mapper.apply(flux, trace, step);
			flux = flux.doOnNext(m -> trace.hit("mapped " + step, m));
		}
		// acking the message
		flux = flux.doOnNext(Message::ack).doOnNext(m -> trace.hit("acked", m));

		// waiting for the flux to be processed
		Message last = flux.last().block();

		// print infos
		LOGGER.info("Latest message : {}", last);
		LOGGER.info("Latest ack : {}", offsets.getNextStart());
		LOGGER.info("Duration : {}", Duration.between(start, Instant.now()));

		trace.printStats();

		List<Long> biggestIntervalsOffsets = offsets.getBiggestIntervalsOffsets();
		for (Long offset : biggestIntervalsOffsets) {
			trace.print(offset);
		}
	}

	public static Publisher<Message> transform(Message input, Trace trace, Integer step) {
		return Mono.just(input).doOnNext(m -> trace.hit("mono " + step, m)).subscribeOn(SCHEDULER_PARALLEL)
				.doOnNext(m -> trace.hit("subscribed " + step, m)).map(i -> {
					long now = System.currentTimeMillis();
					long end = now + TRANSFORM_MIN_DURATION;
					long out = now;
					int n = 0;
					do {
						out = Hashing.sha512().hashLong(out).asLong();
						n++;
					} while (System.currentTimeMillis() <= end);
					LOGGER.debug("{} transformed {} {}", input, step, n);
					return i;
				}).onErrorResume(t -> {
					LOGGER.error("Something went wrong!", t);
					return Mono.empty();
				}).doOnNext(m -> trace.hit("transformed " + step, m));
	}

}
