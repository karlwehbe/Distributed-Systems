package comp512st.paxos;

import comp512.gcl.*;
import comp512.utils.*;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

class Acceptor {
	// Per-round state tracking
	static class RoundState {
		int maxBID = -1;
		int acceptedBID = -1;
		Object acceptedVal = null;
	}
	
	Map<Integer, RoundState> rounds = new HashMap<>();
	int nextDeliverRound = 0;  // Track which round to deliver next
	Map<Integer, Object> confirmBuffer = new TreeMap<>();  // Buffer out-of-order confirms
	
    String processID;
	private final Paxos paxos;
	
	// This thread runs constantly for each player process, reading messages from GCL
	// Messages intended for Proposer (Promise, Deny, AcceptAck, etc.) are put into
	// the proposerQueue, otherwise sends messages to destination processes via GCL
	final Thread thread;
	Logger logger;

	Acceptor(Paxos paxos, String processID, Logger logger) {
		this.paxos = paxos;
        this.processID = processID;
		thread = new Thread(this::run);
		this.logger = logger;
	}
	
	RoundState getOrCreateRound(int round) {
		return rounds.computeIfAbsent(round, k -> new RoundState());
	}

	void run() {
		while (true) {
			GCMessage msg = null;
			try {
                msg = paxos.gcl.readGCMessage(); // block until message arrives
            } catch (InterruptedException e) { 
                Thread.currentThread().interrupt(); break;
            }

		PaxosMessage paxosMsg = (PaxosMessage) msg.val;
		int round = paxosMsg.getRound();
		
		switch (paxosMsg.getType()) { // route message by type
			case PROPOSE: // proposal phase: respond with promise/refuse
				paxos.failCheck.checkFailure(FailCheck.FailureType.RECEIVEPROPOSE);
				promise(round, paxosMsg.ballotID, msg.senderProcess);
				paxos.failCheck.checkFailure(FailCheck.FailureType.AFTERSENDVOTE);
				break;

			case PROMISE: // forward to proposer
				paxos.proposerQueue.offer(paxosMsg);
				break;

			case REFUSE: // forward to proposer
				paxos.proposerQueue.offer(paxosMsg);
				break;

			case ACCEPT: // accept phase: respond with acceptAck/deny
				accept(round, paxosMsg.ballotID, msg.senderProcess, paxosMsg.getVal());
				break;

			case ACCEPTACK: // forward to proposer
				paxos.proposerQueue.offer(paxosMsg);
				break;

			case DENY: // forward to proposer
				paxos.proposerQueue.offer(paxosMsg);
				break;

			case CONFIRM: // deliver decided value to app (in order)
				paxos.proposerQueue.offer(paxosMsg); // Also forward to proposer for early abort
				onConfirm(round, paxosMsg.getVal());
				break;

			default:
				break;
		}
		}
	}

	void promise(int round, int BID, String destProcess) { // proposal phase response
		RoundState state = getOrCreateRound(round);
		if (BID < state.maxBID){
			PaxosMessage refuse = PaxosMessage.refuse(round, state.maxBID, this.processID, destProcess); // reject old ballot
			paxos.gcl.sendMsg(refuse, destProcess);
		} else {
			PaxosMessage promise = PaxosMessage.promise(state.acceptedVal, round, BID, state.acceptedBID, this.processID, destProcess); // promise + return accepted value
			state.maxBID = BID; // update highest seen for this round
			paxos.gcl.sendMsg(promise, destProcess);
		}
	}

	void accept(int round, int BID, String destProcess, Object val) { // accept phase response
		RoundState state = getOrCreateRound(round);
		if (BID == state.maxBID){
			state.acceptedBID = BID; 
			state.acceptedVal = val; // accept the value
			PaxosMessage acceptAck = PaxosMessage.acceptAck(val, round, BID, this.processID, destProcess);
			paxos.gcl.sendMsg(acceptAck, destProcess);
		} else {
			PaxosMessage denial = PaxosMessage.deny(val, round, BID, this.processID, destProcess); // reject non-leader
			paxos.gcl.sendMsg(denial, destProcess);
		}
	}
	
	void onConfirm(int round, Object val) { // Buffer and deliver confirms in order
		confirmBuffer.put(round, val);

		// Deliver all consecutive rounds starting from nextDeliverRound
		while (confirmBuffer.containsKey(nextDeliverRound)) {
			Object deliverVal = confirmBuffer.remove(nextDeliverRound);
			paxos.incoming.offer(deliverVal);
			nextDeliverRound++;
		}

	}
}

