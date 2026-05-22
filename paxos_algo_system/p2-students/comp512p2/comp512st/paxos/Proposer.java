package comp512st.paxos;

import comp512.gcl.*;
import comp512.utils.*;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.Random;
import java.util.Arrays;

import java.util.logging.Logger;

class Proposer {
	// Ballot ID management
	int ballotID;           // Current proposal's ballot ID (unique per process)
    int acceptedBID;        // Highest previously accepted ballot ID seen in promises
    int counter;            // Local counter for generating unique ballot IDs
    int processIndex;       // Process index in group (for uniqueness in ballot IDs)
    String processID;       // This process's identifier
	int currentRound = 0;   // Paxos round/slot this proposer is working on

	// Phase 1 (Propose/Promise) counters
	int promises;           // Number of PROMISE responses received
    int refusals;           // Number of REFUSE responses received
	Object acceptedVal;     // Value to propose (may be adopted from previous round)
	
	// Phase 2 (Accept) counters
	int numAcceptAcks;      // Number of ACCEPTACK responses received
    int numDenies;          // Number of DENY responses received

	private final Paxos paxos;  // Reference to parent Paxos instance for queues/GCL

	final Thread thread;

	Logger logger;

	// For backoff/jitter to reduce contention
	Random random;
	int consecutiveFailures = 0;

	Proposer(Paxos paxos, String myProcess, String[] allGroupProcesses, Logger logger) {
		this.paxos = paxos;
		ballotID = -1;
		counter = 0;
		
		// Find process index in the group
		this.processIndex = -1;
		for (int i = 0; i < allGroupProcesses.length; i++) {
			if (allGroupProcesses[i].equals(myProcess)) {
				this.processIndex = i;
				logger.info("Process " + myProcess + " has index " + i);
				break;
			}
		}
		
		if (this.processIndex == -1) {
			throw new IllegalArgumentException("Process " + myProcess + " not found in group");
		}
		
		this.processID = myProcess;
		promises = 0;
		refusals = 0;
		thread = new Thread(this::run);
		this.logger = logger;
		this.random = new Random();
	}

	void run() {
		while (true){
			// Reset state for new proposal attempt
			acceptedVal = null; 
			acceptedBID = -1;
			
			// Wait for application to submit a value to propose
			Object originalVal = paxos.outgoing.poll();
			Object val = originalVal; 

			if (val != null) {

				if (!paxos.isValuePending(val)) {
					logger.info("PROPOSER: Skipping already-decided value " + val);
					continue;  // Skip this value and poll the next one
				}
				// PHASE 1: PROPOSE - Send proposal to all acceptors
					
				propose();
				paxos.failCheck.checkFailure(FailCheck.FailureType.AFTERSENDPROPOSE);
				paxos.pstartTime = System.currentTimeMillis(); 

				// Phase 1: Propose/Promise, proposer keeps polling for responses until one of these happens:
				// Enough PROMISEs to reach majority (success path)
				// Enough REFUSEs to reach majority (give up and retry)
				// The per-phase timeout (maxDuration) elapses
				// Another proposer wins this round (CONFIRM received)
				// Collect responses until majority reached or timeout
				boolean anotherProposerWon = false;
				while (promises < paxos.majority && refusals < paxos.majority && !anotherProposerWon && (System.currentTimeMillis() - paxos.pstartTime < paxos.maxDuration)) {
					PaxosMessage response = (PaxosMessage) paxos.proposerQueue.poll();
					if (response != null){
						logger.info("PROPOSER: Received " + response.getType() + " round=" + response.getRound() + " ballotID=" + response.ballotID + " from " + response.getSenderProcess());

						// Check if another proposer won this round
						if (response.getType() == PaxosMessage.MsgType.CONFIRM && response.getRound() == this.currentRound) {
							logger.info("PROPOSER: Another proposer won round " + this.currentRound + ", aborting attempt");
							anotherProposerWon = true;
							break; // Abort this attempt
						}

						// Ignore responses for other rounds
						if (response.getRound() != this.currentRound){
							logger.info("PROPOSER: Ignoring message for round " + response.getRound() + " (currentRound=" + this.currentRound + ")");
							continue;
						}
						// Ignore responses for older ballot IDs
						if (response.ballotID < this.ballotID){
							logger.info("PROPOSER: Ignoring older ballot ID " + response.ballotID + " (current=" + this.ballotID + ")");
							continue;
						}

						switch(response.getType()){
							case PROMISE:
								promises += 1;
								logger.info("PROPOSER: Received PROMISE, count=" + promises + "/" + paxos.majority);
								// PAXOS SAFETY: Adopt previously accepted value with highest ballot ID
								if (response.acceptedBID > acceptedBID && response.getVal() != null) { // Checks if the previously accepted value has a higher ballot ID than the current accepted value
									logger.info("PROPOSER: Adopting previously accepted value " + response.getVal() + " with ballotID=" + response.acceptedBID);
									paxos.outgoing.offerFirst(originalVal);  // Requeue original value for later
									acceptedBID = response.acceptedBID;
									val = response.getVal();  // Adopt the previously accepted value in order to propose it again
								}
								break;
							case REFUSE:
								refusals += 1;  // Acceptor rejected our proposal (higher ballot seen)
								logger.info("PROPOSER: Received REFUSE, count=" + refusals + "/" + paxos.majority);
								break;
							default:
								logger.info("PROPOSER: Ignoring message type " + response.getType());
								continue;  // Ignore other message types
							}
					}
				}

				// Check if another proposer won (early abort)
				if (anotherProposerWon) {
				reset();
				backoffOnFailure(); // Backoff before retrying
				if (valuesEqual(val, originalVal)) {
					paxos.outgoing.offerFirst(originalVal);  // This insures that the original value is re-added to the front of the outgoing queue to be proposed again
				}
					currentRound++; // Move to next round since this one is decided
					continue;
				}

				// Check if we achieved majority in proposal phase
				if (promises >= paxos.majority){
					// SUCCESS: We became the leader for this ballot
					paxos.failCheck.checkFailure(FailCheck.FailureType.AFTERBECOMINGLEADER);
					reset();  // Clear proposal phase counters
					
					// PHASE 2: ACCEPT - Send accept message with chosen value
					accept(this.ballotID, val);
					numAcceptAcks = 0; 
					numDenies = 0;  // Reset accept phase counters
					paxos.startTime = System.currentTimeMillis();  

					// Collect accept responses until majority reached or timeout
					anotherProposerWon = false;
					while (numAcceptAcks < paxos.majority && numDenies < paxos.majority && !anotherProposerWon && (System.currentTimeMillis() - paxos.startTime < paxos.maxDuration)){
						PaxosMessage response = (PaxosMessage) paxos.proposerQueue.poll();
						if (response != null) {
							logger.info("PROPOSER: Received " + response.getType() + " round=" + response.getRound() + " ballotID=" + response.ballotID + " from " + response.getSenderProcess());

							// Check if another proposer won this round
							if (response.getType() == PaxosMessage.MsgType.CONFIRM && response.getRound() == this.currentRound) {
								logger.info("PROPOSER: Another proposer won round " + this.currentRound + " during accept phase, aborting");
								anotherProposerWon = true;
								break;
							}

							// Ignore responses for other rounds
							if (response.getRound() != this.currentRound){
								logger.info("PROPOSER: Ignoring message for round " + response.getRound() + " (currentRound=" + this.currentRound + ")");
                                continue;
                            }
							// Ignore responses for older ballot IDs
							if (response.ballotID < this.ballotID){
								logger.info("PROPOSER: Ignoring older ballot ID " + response.ballotID + " (current=" + this.ballotID + ")");
                                continue;
                            }

                            switch (response.getType()) {
								case ACCEPTACK:
									numAcceptAcks += 1;  // Acceptor accepted our value
									logger.info("PROPOSER: Received ACCEPTACK, count=" + numAcceptAcks + "/" + paxos.majority);
									break;

								case DENY:
									numDenies += 1;  // Acceptor denied our accept (higher ballot seen)
									logger.info("PROPOSER: Received DENY, count=" + numDenies + "/" + paxos.majority);
									break;

								default:
									logger.info("PROPOSER: Ignoring message type " + response.getType());
									continue;  // Ignore other message types
							}
						}
					}


					// Check if another proposer won during accept phase (early abort)
					if (anotherProposerWon) {
					reset();
					backoffOnFailure(); // Backoff before retrying
					if (paxos.isValuePending(originalVal)) {    // Checks if the original value (that was initially submitted by the application) is still pending
						paxos.outgoing.offerFirst(originalVal);  // Re-adds the value to the front of the outgoing queue to be proposed again	
					}
						currentRound++; // Move to next round since this one is decided
					}
					// Check if we achieved majority in accept phase
					else if (numAcceptAcks >= paxos.majority) {
						
											
						// SUCCESS: Value accepted by majority - broadcast CONFIRM
						paxos.failCheck.checkFailure(FailCheck.FailureType.AFTERVALUEACCEPT);
						acceptedVal = val;

						confirm(this.ballotID, this.acceptedVal);  // PHASE 3: CONFIRM
						reset();
						consecutiveFailures = 0; // Reset on success
						currentRound++;  // Move to next round after successful consensus
						
					// Notify that this value is decided (unblock broadcastTOMsg)
					if (valuesEqual(val, originalVal)) {		// Compares the decided value (val) with the original value (originalVal) that was submitted by the application
						paxos.notifyValueDecided(originalVal);		// Notifies the Paxos system that this value has been decided and unblocks the broadcastTOMsg() method
					}

					} else {
						// FAILURE: Not enough acceptances - retry later
						reset();
						backoffOnFailure(); // Backoff before retrying
						paxos.outgoing.offerFirst(originalVal);  // Requeue for retry
					}

				} else {
					// FAILURE: Not enough promises in proposal phase - retry later
					reset();
					backoffOnFailure(); // Backoff before retrying
					paxos.outgoing.offerFirst(originalVal);  // Requeue for retry
				}
			} else { 
                continue; 
            }
		}
	}

	int generateBID() { // Makes a unique, monotonically increasing ballot ID by combining counter with process index
		counter += 1;
		int bid = (counter << 4) | processIndex; // 28 bits for counter, 4 bits for process (max 16 processes)
		this.logger.info("PROPOSER: Generated ballot ID: " + bid);
		return bid;
	}
	
	// Helper method to compare values (handles Object[] arrays correctly)
	boolean valuesEqual(Object v1, Object v2) {
		if (v1 == v2) return true;
		if (v1 == null || v2 == null) return false;
		if (v1 instanceof Object[] && v2 instanceof Object[]) {
			return Arrays.equals((Object[]) v1, (Object[]) v2);
		}
		return v1.equals(v2);
	}

	void backoffOnFailure() {
		// Exponential backoff with jitter to reduce contention
		consecutiveFailures++;
		if (consecutiveFailures > 0) {
			// Base backoff: 2^failures * 10ms, capped at 200ms
			int baseBackoff = Math.min(200, (int) Math.pow(2, Math.min(consecutiveFailures, 5)) * 10);
			// Add random jitter (0-50% of base)
			int jitter = random.nextInt(baseBackoff / 2);
			int totalBackoff = baseBackoff + jitter;

			logger.info("PROPOSER: Backing off for " + totalBackoff + "ms (failure count=" + consecutiveFailures + ")");
			try {
				Thread.sleep(totalBackoff);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

	void propose() { // Sets a new ballot ID and broadcasts a PROPOSE message via GCL to start the proposal phase.
		ballotID = generateBID();
		PaxosMessage proposal = PaxosMessage.propose(currentRound, ballotID, this.processID);
		logger.info("PROPOSER: Sending PROPOSE round=" + currentRound + " ballotID=" + ballotID);
		paxos.gcl.broadcastMsg(proposal);
	}

	void reset() { // Clears per-attempt state (promise/refuse and accept/deny counters) and the proposerQueue before a retry or next phase.
		promises = 0;
		refusals = 0;
		numAcceptAcks = 0;
		numDenies = 0;
		paxos.proposerQueue.clear();
	}

	void accept(int ballotID, Object val) { //  Broadcasts an ACCEPT message with the chosen value and current ballot ID to run the accept phase.
		PaxosMessage acceptq = PaxosMessage.accept(val, currentRound, ballotID, this.processID);
		logger.info("PROPOSER: Sending ACCEPT round=" + currentRound + " ballotID=" + ballotID + " value=" + val);
		paxos.gcl.broadcastMsg(acceptq);
	}

	void confirm(int BID, Object val) { // Broadcasts a CONFIRM message so all processes deliver the decided value to their application layer.
		PaxosMessage confirmation = PaxosMessage.confirm(val, currentRound, BID, processID);
		logger.info("PROPOSER: Sending CONFIRM round=" + currentRound + " ballotID=" + BID + " value=" + val);
		paxos.gcl.broadcastMsg(confirmation);
	}

}


