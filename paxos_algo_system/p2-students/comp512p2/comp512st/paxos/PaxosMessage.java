package comp512st.paxos;

import java.io.Serializable;

class PaxosMessage implements Serializable {
	static enum MsgType {PROPOSE, PROMISE, REFUSE, ACCEPT, ACCEPTACK, DENY, CONFIRM}

	private Object val;
	private String senderProcess;  // Who created/sent this message
	private String destinationProcess;  // Who should receive this message (for point-to-point)
	private MsgType type;
	int ballotID;
	int acceptedBID;
	int round;

	PaxosMessage(MsgType type, Object val, int round, int ballotID, int acceptedBID, String senderProcess, String destinationProcess) {
		this.val = val;
		this.type = type;
		this.round = round;
		this.ballotID = ballotID;
		this.acceptedBID = acceptedBID;
		this.senderProcess = senderProcess;
		this.destinationProcess = destinationProcess;
	}

	// Factory methods to avoid passing unused args at call sites
	static PaxosMessage propose(int round, int ballotID, String senderProcess) {
		return new PaxosMessage(MsgType.PROPOSE, null, round, ballotID, -1, senderProcess, null);
	}

	static PaxosMessage accept(Object val, int round, int ballotID, String senderProcess) {
		return new PaxosMessage(MsgType.ACCEPT, val, round, ballotID, -1, senderProcess, null);
	}

	static PaxosMessage confirm(Object val, int round, int ballotID, String senderProcess) {
		return new PaxosMessage(MsgType.CONFIRM, val, round, ballotID, -1, senderProcess, null);
	}


	static PaxosMessage promise(Object acceptedVal, int round, int ballotID, int acceptedBID, String senderProcess, String destinationProcess) {
		return new PaxosMessage(MsgType.PROMISE, acceptedVal, round, ballotID, acceptedBID, senderProcess, destinationProcess);
	}

	static PaxosMessage refuse(int round, int maxKnownBallotID, String senderProcess, String destinationProcess) {
		return new PaxosMessage(MsgType.REFUSE, null, round, maxKnownBallotID, -1, senderProcess, destinationProcess);
	}

	static PaxosMessage acceptAck(Object val, int round, int ballotID, String senderProcess, String destinationProcess) {
		return new PaxosMessage(MsgType.ACCEPTACK, val, round, ballotID, -1, senderProcess, destinationProcess);
	}

	static PaxosMessage deny(Object val, int round, int ballotID, String senderProcess, String destinationProcess) {
		return new PaxosMessage(MsgType.DENY, val, round, ballotID, -1, senderProcess, destinationProcess);
	}

	

	String getDestinationProcess() {
		return this.destinationProcess;
	}

	Object getVal() {
		return this.val;
	}

	MsgType getType() {
		return this.type;
	}

	String getSenderProcess() {
		return this.senderProcess;
	}

	int getRound() {
		return this.round;
	}
}

