package com.github.glandais.reactor;

public interface Trace {

	void hit(String where, Message message);

	void printStats();

	void print(Long key, Long lagCount);

}