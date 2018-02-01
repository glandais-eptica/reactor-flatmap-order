package com.github.glandais.reactor;

import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

public interface Markers {

	Marker ALWAYS = MarkerFactory.getMarker("ALWAYS");

	Marker STATS = MarkerFactory.getMarker("STATS");

	Marker LAGS = MarkerFactory.getMarker("LAGS");

	Marker ACK = MarkerFactory.getMarker("ACK");

}
