package comp512st.paxos;
import comp512.gcl.*;
import comp512.utils.*;

import java.io.*;
import java.util.logging.*;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.logging.Logger;


public class Paxos
{
	// Wrapper class for array values to work correctly as HashMap keys
	private static class ValueWrapper {
		private final Object value;
		private final int hash;
		
		ValueWrapper(Object value) {
			this.value = value;
			if (value instanceof Object[]) {
				this.hash = Arrays.hashCode((Object[]) value);
			} else {
				this.hash = (value != null) ? value.hashCode() : 0;
			}
		}
		
		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (!(obj instanceof ValueWrapper)) return false;
			ValueWrapper other = (ValueWrapper) obj;
			if (value instanceof Object[] && other.value instanceof Object[]) {
				return Arrays.equals((Object[]) value, (Object[]) other.value);
			}
			return (value != null) ? value.equals(other.value) : other.value == null;
		}
		
		@Override
		public int hashCode() {
			return hash;
		}
	}
	
	GCL gcl;
	FailCheck failCheck;

	// Queues connecting app, proposer and acceptor
	LinkedBlockingDeque<Object> outgoing;      // app to proposer queue (values to propose)
	LinkedBlockingDeque<Object> incoming;      // acceptor to app queue (decided values)
	LinkedBlockingDeque<Object> proposerQueue; // acceptor to proposer queue (protocol replies)

	String processName;
	int maxMoveInterval;

	ConcurrentHashMap<Object, Long> latencyTracker = new ConcurrentHashMap<>(); // Track latency
	long throughputStartTime = System.currentTimeMillis(); // Track throughput


	// Track pending proposals so broadcastTOMsg() can block until decided
	ConcurrentHashMap<ValueWrapper, CountDownLatch> pendingProposals;	// Thread-safe hashmap that tracks pending proposals

	Proposer proposer;  // sends propose/accept/confirm
	Acceptor acceptor;  // handles propose/accept, forwards replies

	// Minimal timing used for proposer timeouts (per-phase)
	long startTime;   // accept phase start time
	long pstartTime;  // proposal phase start time
	long maxDuration = TimeUnit.MILLISECONDS.toMillis(1000); // Timeout, in case majority not received

	int numProcesses; 
	int majority;

	Logger logger;

	public Paxos(String myProcess, String[] allGroupProcesses, Logger logger, FailCheck failCheck) throws IOException, UnknownHostException
	{
		this.processName = myProcess;
		this.failCheck = failCheck;
		this.logger = logger;
		this.gcl = new GCL(myProcess, allGroupProcesses, null, logger);
		numProcesses = allGroupProcesses.length;
		majority = (numProcesses / 2)+1;
		outgoing = new LinkedBlockingDeque<>();
		incoming = new LinkedBlockingDeque<>();
		proposerQueue = new LinkedBlockingDeque<>();
		pendingProposals = new ConcurrentHashMap<>();
		acceptor = new Acceptor(this, myProcess, logger);
		proposer = new Proposer(this, myProcess, allGroupProcesses, logger);
		proposer.thread.start();  // start protocol threads
		acceptor.thread.start();


	}

	// Helper method for Proposer to check if a value is still pending
	boolean isValuePending(Object val) {
		return pendingProposals.containsKey(new ValueWrapper(val));
	}
	
	// Called by Proposer when a value is decided (majority accepts)
	void notifyValueDecided(Object val) {
		CountDownLatch latch = pendingProposals.remove(new ValueWrapper(val));
		if (latch != null) {
			latch.countDown(); // Unblock broadcastTOMsg()
		}
	}

	// This is what the application layer is going to call to send a message/value, such as the player and the move
	// BLOCKS until value is accepted by majority (but not necessarily all processes)
	public void broadcastTOMsg(Object val)
	{
		// Track start time of move being processed
		long performanceAnalysis1Start = System.currentTimeMillis();
		latencyTracker.computeIfAbsent(val, key -> performanceAnalysis1Start);

		// Create a latch that will be released when this value is decided
		CountDownLatch latch = new CountDownLatch(1);
		pendingProposals.put(new ValueWrapper(val), latch);

		outgoing.offer(val); // enqueue value to be proposed

		// Block until the value is decided by majority
		try {
			latch.await(); // Wait for proposer to count down
		} catch (InterruptedException e) {
			logger.warning("broadcastTOMsg interrupted: " + e.getMessage());
			Thread.currentThread().interrupt();
		}
		
		// Calculate latency after consensus is reached
		long endTime = System.currentTimeMillis();
		latencyTracker.put(val, endTime - startTime);
	}


	public Object acceptTOMsg() throws InterruptedException
	{
		return incoming.take(); // take blocks until a value is decided
	}

	private synchronized void writeMetrics(String val, long latency, int x_axis_value, double throughput, boolean vary_player_count) {
		if(vary_player_count) {
			try (FileWriter fw = new FileWriter("metrics_varying_players.csv", true)) {
				fw.write(val + "," + latency + "," + throughput + "," + x_axis_value + "\n"); // For varying player count
			} 
			catch (IOException e) {
				e.printStackTrace();
			}

		}
		else {
			// try (FileWriter fw = new FileWriter("metrics_varying_interval.csv", true)) { // For 3 Player testing
			try (FileWriter fw = new FileWriter("metrics_varying_interval_2.csv", true)) {
				fw.write(val + "," + latency + "," + throughput + "," + x_axis_value + "\n"); // For varying player count
			} 
			catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	// analysis: tracking acceptance latency and throughput as a function of 
	// player count and time interval size
	void performanceAnalysis(long duration_for_throughput) {
		long avg = 0;
		List<Long> latencies = new ArrayList<>(latencyTracker.values());

		// Remove latencies from warmup and cooldown period (roughly 5 from each)
		List<Long> latencies_final = latencies.subList(5, latencies.size()-5);

		for(Long l : latencies_final) { avg += l; }
		avg /= latencies_final.size();

		double numMoves = 100.0;
		double throughput = numMoves / duration_for_throughput;
    
		//writeMetrics(processName, avg, numProcesses, throughput, true); // Write to metrics_varying_players.csv
		writeMetrics(processName, avg, 10, throughput, false); // Write to metrics_varying_intervals.csv
		//writeMetrics(processName, avg, numProcesses, throughput, true); // Write to metrics_varying_players.csv
		writeMetrics(processName, avg, 100, throughput, false); // Write to metrics_varying_intervals.csv
	}


	public void shutdownPaxos()
	{
		// Graceful shutdown by waiting for queues to drain, then stop GCL
		long duration_for_throughput = System.currentTimeMillis() - throughputStartTime; 
		Thread monitorThread = new Thread(() -> {
			try {
				while (true) {
					if (incoming.isEmpty() && outgoing.isEmpty() && proposerQueue.isEmpty()) {
						System.out.println("Shutting down Paxos");
						break; // Exit the loop and end the thread
					} 
					Thread.sleep(100); 
				}
			} catch (InterruptedException e) {
				System.out.println("Error shutting down Paxos");
			}
		});

		monitorThread.start(); // start thread to allow queues to drain before shutdown

		// performanceAnalysis(duration_for_throughput/1000L);

		gcl.shutdownGCL();
	}


}