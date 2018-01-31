package com.github.glandais.reactor;

import java.util.function.BiFunction;

public interface Transform extends BiFunction<Message, Integer, Message> {

}
