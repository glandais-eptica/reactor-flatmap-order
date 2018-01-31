package com.github.glandais.reactor;

public class Message {

	private Long offset;
	private Offsets offsets;

	public Message(Long offset, Offsets offsets) {
		super();
		this.offset = offset;
		this.offsets = offsets;
	}

	public Long offset() {
		return offset;
	}

	@Override
	public String toString() {
		return "_" + offset + "_";
	}

	public void ack() {
		this.offsets.checkAck(this);
	}

}
