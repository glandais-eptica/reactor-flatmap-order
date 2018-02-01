package com.github.glandais.reactor;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import org.jooq.lambda.Seq;
import org.jooq.lambda.tuple.Tuple2;
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
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.function.Function4;

public class Main {

	private static final Scheduler SCHEDULER_MAIN = Schedulers.newElastic("main");

	private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

	// Number of items
	public static final int COUNT = 3000;

	protected static final long DURATION_IO = 20L;

	protected static final long CPU_CYCLES = 5000;

	// Transformers
	protected static final Transform[] TRANSFORMS = new Transform[] { new TransformIO(DURATION_IO),
			new TransformCPU(CPU_CYCLES), new TransformCPU(CPU_CYCLES), new TransformIO(DURATION_IO) };

	public static final Level LEVEL = Level.FINE;

	public static void main(String[] args) {
		// testing various flatMap alikes
		Main main = new Main();

		//		main.bench("map", main::map, new OffsetsFakeImpl(), new TraceRealImpl());

		//		main.bench("flatMap0 F F", main::flatMap, new OffsetsFakeImpl(), new TraceFakeImpl());
		//		main.bench("flatMap0 F R", main::flatMap, new OffsetsFakeImpl(), new TraceRealImpl());
		//		main.bench("flatMap0 R F", main::flatMap, new OffsetsRealImpl(0L), new TraceFakeImpl());
		main.bench("flatMap0 R R", main::flatMap, new OffsetsRealImpl(0L), new TraceRealImpl());
		//		main.bench("flatMap1 F F", main::flatMap, new OffsetsFakeImpl(), new TraceFakeImpl());
		//		main.bench("flatMap1 F R", main::flatMap, new OffsetsFakeImpl(), new TraceRealImpl());
		//		main.bench("flatMap1 R F", main::flatMap, new OffsetsRealImpl(0L), new TraceFakeImpl());
		//		main.bench("flatMap1 R R", main::flatMap, new OffsetsRealImpl(0L), new TraceRealImpl());

		//		main.bench("concatMap", main::concatMap, new OffsetsFakeImpl(), new TraceRealImpl());
		//		main.bench("flatMapSequential", main::flatMapSequential, new OffsetsFakeImpl(), new TraceRealImpl());

		SCHEDULER_MAIN.dispose();
		for (Transform transform : TRANSFORMS) {
			transform.stopScheduler();
		}
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

	private void bench(String name, Function4<Flux<Message>, Transform, Trace, Integer, Flux<Message>> mapper,
			Offsets offsets, Trace trace) {
		LOGGER.warn(Markers.ALWAYS, "****************** {} ******************", name);

		// some context
		Instant start = Instant.now();
		AtomicLong acks = new AtomicLong(0L);
		Stopwatch stopwatch = Stopwatch.createUnstarted();

		// generate a list of message
		Flux<Message> flux = Flux.range(0, COUNT).subscribeOn(SCHEDULER_MAIN).map(i -> new Message((long) i, offsets))
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
		LOGGER.info(Markers.ALWAYS, "Latest message : {}", last);
		LOGGER.debug(Markers.ACK, "Latest ack : {}", offsets.getNextStart());
		LOGGER.debug(Markers.ACK, "Acks count : {}", acks);
		LOGGER.debug(Markers.ACK, "Acks duration : {}", stopwatch);
		LOGGER.info(Markers.ALWAYS, "Duration : {}", Duration.between(start, Instant.now()));

		trace.printStats();

		List<Tuple2<Long, Long>> biggestIntervalsOffsets = offsets.getBiggestIntervalsOffsets();
		if (!biggestIntervalsOffsets.isEmpty()) {
			LOGGER.warn(Markers.LAGS, "Biggest laggers : ");
		}
		Seq.zipWithIndex(biggestIntervalsOffsets).forEach(t -> {
			LOGGER.warn(Markers.LAGS, "Lagger #{}", (t.v2 + 1));
			trace.print(t.v1.v1, t.v1.v2);
		});
	}

	public Publisher<Message> transform(Message input, Transform transform, Trace trace, Integer step) {
		trace.hit("transform " + step, input);
		return Mono.just(input).doOnNext(m -> trace.hit("mono " + step, m)).subscribeOn(transform.scheduler())
				.doOnNext(m -> trace.hit("subscribedOn " + step, m)).map(i -> transform.apply(i, step))
				.doOnNext(m -> trace.hit("transformed " + step, m)).onErrorResume(t -> {
					LOGGER.error("Something went wrong!", t);
					input.ack();
					return Mono.empty();
				});
	}

}
