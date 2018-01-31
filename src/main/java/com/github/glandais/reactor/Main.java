package com.github.glandais.reactor;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.logging.Level;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.glandais.reactor.impl.OffsetsRealImpl;
import com.github.glandais.reactor.impl.TraceRealImpl;
import com.github.glandais.reactor.impl.TransformCPU;
import com.github.glandais.reactor.impl.TransformIO;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.function.Function4;

public class Main {

	private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

	// Number of items
	public static final int COUNT = 1000;

	// Transformers
	protected static final Transform[] TRANSFORMS = new Transform[] { new TransformIO(20L), new TransformCPU(5L),
			new TransformIO(20L) };

	public static final Level LEVEL = Level.FINE;

	public static final Scheduler SCHEDULER_PARALLEL = Schedulers.newParallel("sched");

	public static void main(String[] args) {
		// testing various flatMap alikes

		bench("flatMap", Main::flatMap);
		//		bench("concatMap", Main::concatMap);
		bench("flatMapSequential", Main::flatMapSequential);

		SCHEDULER_PARALLEL.dispose();
	}

	public static Flux<Message> flatMap(Flux<Message> flux, Transform transform, Trace trace, Integer step) {
		return flux.flatMap(m -> transform(m, transform, trace, step));
	}

	public static Flux<Message> concatMap(Flux<Message> flux, Transform transform, Trace trace, Integer step) {
		return flux.concatMap(m -> transform(m, transform, trace, step));
	}

	public static Flux<Message> flatMapSequential(Flux<Message> flux, Transform transform, Trace trace, Integer step) {
		return flux.flatMapSequential(m -> transform(m, transform, trace, step));
	}

	private static void bench(String name, Function4<Flux<Message>, Transform, Trace, Integer, Flux<Message>> mapper) {
		LOGGER.info("****************** {} ******************", name);

		// some context
		Instant start = Instant.now();
		Offsets offsets = new OffsetsRealImpl(0);
		Trace trace = new TraceRealImpl();
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
		flux = flux.doOnNext(Message::ack).doOnNext(m -> trace.hit("acked", m));

		// waiting for the flux to be processed
		Message last = flux.last().block();

		// print infos
		LOGGER.info("Latest message : {}", last);
		LOGGER.info("Latest ack : {}", offsets.getNextStart());
		LOGGER.info("Duration : {}", Duration.between(start, Instant.now()));

		trace.printStats();

		Map<Long, Long> biggestIntervalsOffsets = offsets.getBiggestIntervalsOffsets();
		biggestIntervalsOffsets.forEach((offset, lag) -> {
			LOGGER.info("{} (-{} lag)", offset, lag);
			trace.print(offset);
		});
	}

	public static Publisher<Message> transform(Message input, Transform transform, Trace trace, Integer step) {
		return Mono.just(input).doOnNext(m -> trace.hit("mono " + step, m)).subscribeOn(SCHEDULER_PARALLEL)
				.doOnNext(m -> trace.hit("subscribedOn " + step, m)).map(i -> {
					return transform.apply(i, step);
				}).onErrorResume(t -> {
					LOGGER.error("Something went wrong!", t);
					input.ack();
					return Mono.empty();
				}).doOnNext(m -> trace.hit("transformed " + step, m));
	}

}
