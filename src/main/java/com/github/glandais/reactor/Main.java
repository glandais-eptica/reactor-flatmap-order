package com.github.glandais.reactor;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.glandais.reactor.impl.OffsetsRealImpl;
import com.github.glandais.reactor.impl.TraceRealImpl;
import com.github.glandais.reactor.impl.TransformCPU;
import com.github.glandais.reactor.impl.TransformIO;
import com.google.common.base.Stopwatch;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.function.Function4;

public class Main {

	private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

	// Number of items
	public static final int COUNT = 1000;

	// Transformers
	protected static final Transform[] TRANSFORMS = new Transform[] { new TransformIO(20L), new TransformCPU(50L),
			new TransformIO(20L) };

	public static final Level LEVEL = Level.FINE;

	public static void main(String[] args) {
		// testing various flatMap alikes
		Main main = new Main();

		//		main.bench("map", main::map);
		main.bench("flatMap", main::flatMap);
		//		main.bench("concatMap", main::concatMap);
		//		main.bench("flatMapSequential", main::flatMapSequential);

		Schedulers.shutdownNow();
	}

	public Flux<Message> map(Flux<Message> flux, Transform transform, Trace trace, Integer step) {
		return flux.parallel().runOn(transform.scheduler()).map(m -> transform.apply(m, step)).sequential();
	}

	public Flux<Message> flatMap(Flux<Message> flux, Transform transform, Trace trace, Integer step) {
		return flux.flatMap(m -> transform(m, transform, trace, step));
	}

	public Flux<Message> concatMap(Flux<Message> flux, Transform transform, Trace trace, Integer step) {
		return flux.concatMap(m -> transform(m, transform, trace, step));
	}

	public Flux<Message> flatMapSequential(Flux<Message> flux, Transform transform, Trace trace, Integer step) {
		return flux.flatMapSequential(m -> transform(m, transform, trace, step));
	}

	private void bench(String name, Function4<Flux<Message>, Transform, Trace, Integer, Flux<Message>> mapper) {
		LOGGER.info("****************** {} ******************", name);

		// some context
		Instant start = Instant.now();
		Offsets offsets = new OffsetsRealImpl(0);
		Trace trace = new TraceRealImpl();
		AtomicLong acks = new AtomicLong(0L);
		Stopwatch stopwatch = Stopwatch.createUnstarted();
		//		Offsets offsets = new OffsetsFakeImpl();
		//		Trace trace = new TraceFakeImpl();

		// generate a list of message
		Flux<Message> flux = Flux.range(0, COUNT).map(i -> new Message((long) i, offsets))
				.doOnNext(m -> trace.hit("sourced", m));

		// mapping flux using provided mapper
		int i = 0;
		for (Transform transform : TRANSFORMS) {
			int step = i;
			flux = mapper.apply(flux, transform, trace, step);
			flux = flux.doOnNext(m -> trace.hit("mapped " + step, m));
			i++;
		}
		// acking the message
		flux = flux.doOnNext(message -> {
			stopwatch.start();
			message.ack();
			stopwatch.stop();
			acks.incrementAndGet();
		}).doOnNext(m -> trace.hit("acked", m));

		// waiting for the flux to be processed
		Message last = flux.last().block();

		// print infos
		LOGGER.info("Latest message : {}", last);
		LOGGER.info("Latest ack : {}", offsets.getNextStart());
		LOGGER.info("Acks count : {}", acks);
		LOGGER.info("Acks duration : {}", stopwatch);
		LOGGER.info("Duration : {}", Duration.between(start, Instant.now()));

		trace.printStats();

		Map<Long, Long> biggestIntervalsOffsets = offsets.getBiggestIntervalsOffsets();
		biggestIntervalsOffsets.forEach((offset, lag) -> {
			LOGGER.info("{} ({} lags)", offset, lag);
			trace.print(offset);
		});
	}

	public Publisher<Message> transform(Message input, Transform transform, Trace trace, Integer step) {
		return Mono.just(input).doOnNext(m -> trace.hit("mono " + step, m)).subscribeOn(transform.scheduler())
				.doOnNext(m -> trace.hit("subscribedOn " + step, m)).map(i -> {
					return transform.apply(i, step);
				}).onErrorResume(t -> {
					LOGGER.error("Something went wrong!", t);
					input.ack();
					return Mono.empty();
				}).doOnNext(m -> trace.hit("transformed " + step, m));
	}

}
