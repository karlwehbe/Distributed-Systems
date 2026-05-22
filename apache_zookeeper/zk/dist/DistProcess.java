/*
Copyright
All materials provided to the students as part of this course is the property of respective authors. Publishing them to third-party (including websites) is prohibited. Students may save it for their personal use, indefinitely, including personal cloud storage spaces. Further, no assessments published as part of this course may be shared with anyone else. Violators of this copyright infringement may face legal actions in addition to the University disciplinary proceedings.
©2022, Joseph D’Silva; ©2024, Bettina Kemme; ©2025, Olivier Michaud
*/
import java.io.*;

import java.util.*;

// To get the name of the host.
import java.net.*;

//To get the process id.
import java.lang.management.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;
import org.apache.zookeeper.data.Stat;

import java.util.*;
import java.util.stream.Collectors;

// TODO
// Replace XX with your group number.
// You may have to add other interfaces such as for threading, etc., as needed.
// This class will contain the logic for both your manager process as well as the worker processes.
//  Make sure that the callbacks and watch do not conflict between your manager's logic and worker's logic.
//		This is important as both the manager and worker may need same kind of callbacks and could result
//			with the same callback functions.
//	For simplicity, so far all the code in a single class (including the callbacks).
//		You are free to break it apart into multiple classes, if that is your programming style or helps
//		you manage the code more modularly.
//	REMEMBER !! Managers and Workers are also clients of ZK and the ZK client library is single thread - Watches & CallBacks should not be used for time consuming tasks.
//		In particular, if the process is a worker, Watches & CallBacks should only be used to assign the "work" to a separate thread inside your program.
public class DistProcess implements Watcher, AsyncCallback.ChildrenCallback
{
    ZooKeeper zk;
    String zkServer, pinfo;
    boolean isManager=false;
    boolean initialized=false;
    List<String> workers;
    Queue<String> availableWorkers;
    Queue<String> tasksToDo;
    Set<String> tasksCompleted;
    //Set<String> allTasks;
    String workerName = "manager"; // Becomes worker's name IF the DistProcess is a worker
    Set<String> processedAssignments; // Track assignments already being processed (for workers)

    int timeslice = 1000; // time slice for each task in milliseconds

    DistProcess(String zkhost)
    {
        zkServer=zkhost;
        pinfo = ManagementFactory.getRuntimeMXBean().getName();
        System.out.println("DISTAPP : ZK Connection information : " + zkServer);
        System.out.println("DISTAPP : Process information : " + pinfo);
    }

    void startProcess() throws IOException, UnknownHostException, KeeperException, InterruptedException
    {
        zk = new ZooKeeper(zkServer, 10000, this); //connect to ZK. Originally 10000
    }

    void initialize()
    {
        try
        {
            runForManager();	// See if you can become the manager (i.e, no other manager exists)
            isManager=true;
            workers = new ArrayList(); // Init arraylist to keep track of worker znodes
            availableWorkers = new LinkedList();
            tasksToDo = new LinkedList<>();
            //allTasks = new HashSet();
            tasksCompleted = new HashSet();

            // Since tasks have been predefined as SEQUENTIAL_PERSISTENT, the
            // manager starts by first getting any prior tasks 
            // This will help us with filtering out and finding newly created tasks later

            getTasks(); // Install monitoring on any new tasks that will be created.
            
            // TODO monitor for worker tasks?

            zk.getChildren("/dist38/workers", this); // Set watch to check if new workers added

            // Also note, as part ofour one-time setup we create a /dist38/assignments persistent znode.
            // This znode has ephermal children znodes corresponding to an active worker i.e.
            // a worker which currently has a task assigned to it
            // This way we can track which workers are available to process task
            zk.getChildren("/dist38/assignments", this);

        }catch(NodeExistsException nee)
        { 
            // TODO: What else will you need if this was a worker process?
            isManager=false; 
            System.out.println(
                "Failed to become manager; adding this process as a child to /dist/workers..."
            );
            try {
                // Add as sequential znode to /dist/workers
                // Use ephermal sequential nodes
                // Sequential because can have multiple workers, makes it easier to add new ones
                // Ephermal because workers are temporary: same DistProcess is not always
                // going to be a worker, could sometimes be a manager
                String newWorkerName = zk.create("/dist38/workers/worker-", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

                // Also set a watcher for each worker DistProcess
                // This makes sure a worker does not miss assignments given by the manager
                // We create this worker znode as a child of /dist38/assignments when 
                // this worker znode receives a new task from manager

                // Note that the manager watches /dist38/assignments to check for new children
                // (in order to track which workers are already being used and are thus unavailable)
                // and each worker is also watching, but instead it watches for the EXISTENCE of 
                // its node as a child of /dist38/assignments to see if it has been given a task

                String workerName = newWorkerName.substring(newWorkerName.lastIndexOf('/') + 1);
                this.workerName = workerName;
                System.out.println("DISTAPP : (Worker) I am " + this.workerName);

                // Initialize the set to track processed assignments
                processedAssignments = new HashSet<>();

                String workerAssignmentPath = "/dist38/assignments";

                zk.getChildren(workerAssignmentPath, this);      
            } catch (Exception e) {
                System.err.println("There was an issue adding current process to /dist/workers");
            }

        } 
        catch(UnknownHostException uhe)
        { System.out.println(uhe); }
        catch(KeeperException ke)
        { System.out.println(ke); }
        catch(InterruptedException ie)
        { System.out.println(ie); }

        System.out.println("DISTAPP : Role : " + " I will be functioning as " +(isManager?"manager":"worker"));

    }

    // Helper functions to parse assignment names
    String getAssignmentWorker(String assignmentName) {
        String[] parts = assignmentName.split("-");
        String worker = parts[0] + "-" + parts[1];  // worker-XXXXXXX
        String task = parts[2] + "-" + parts[3];    // task-YYYYYYY
        return worker;
    }

    String getAssignmentTask(String assignmentName) {
        String[] parts = assignmentName.split("-");
        String worker = parts[0] + "-" + parts[1];  // worker-XXXXXXX
        String task = parts[2] + "-" + parts[3];    // task-YYYYYYY
        return task;
    }

    // Helper function to check if a task already has a result (is completed)
    private boolean taskHasResult(String taskName) {
        try {
            String resultPath = "/dist38/tasks/" + taskName + "/result";
            Stat stat = zk.exists(resultPath, false);
            return stat != null;
        } catch (KeeperException | InterruptedException e) {
            return false;
        }
    }

    // Helper function to check if a task znode still exists
    private boolean taskExists(String taskName) {
        try {
            String taskPath = "/dist38/tasks/" + taskName;
            Stat stat = zk.exists(taskPath, false);
            return stat != null;
        } catch (KeeperException | InterruptedException e) {
            return false;
        }
    }

    private void assignWaitingTask(String taskName, String workerName) {
        String assignmentPath = "/dist38/assignments/"+workerName+"-"+taskName;
        try {
            // Get the latest task data (might be updated if task was previously interrupted)
            byte[] taskData = zk.getData("/dist38/tasks/"+taskName, false, null);
            System.out.println("DISTAPP : Creating new assignment: " + assignmentPath);
            zk.create(assignmentPath, taskData, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException ke) {
            System.out.println("Could not assign waiting task " + taskName + " to worker " + workerName);
            // Return worker to available pool and task to waiting queue
            availableWorkers.add(workerName);
            if (!tasksToDo.contains(taskName)) {
                tasksToDo.add(taskName);
            }
            tasksCompleted.remove(taskName);
        } catch(InterruptedException ie) {
            System.out.println("Interrupt while trying to assign a waiting task");
            // Return worker to available pool and task to waiting queue
            availableWorkers.add(workerName);
            if (!tasksToDo.contains(taskName)) {
                tasksToDo.add(taskName);
            }
            tasksCompleted.remove(taskName);
        }
    }

    private void computeTaskResult(String assignment) {
        // Get the assignment
        String assignmentPath = "/dist38/assignments/"+assignment;
        String taskName = this.getAssignmentTask(assignment);
        Thread workerThread = Thread.currentThread();

        System.out.println("DISTAPP : ("+workerName+") STARTING computation for " + taskName + " (Thread: " + Thread.currentThread().getName() + ")");

        try {
            byte[] taskSerial = zk.getData(assignmentPath, false, null);

            // Re-construct our task object.
            ByteArrayInputStream bis = new ByteArrayInputStream(taskSerial);
            ObjectInput in = new ObjectInputStream(bis);
            DistTask dt = (DistTask) in.readObject();

            // Start a timer thread that will interrupt the worker thread after time slice expires
            Thread timerThread = new Thread(() -> {
                try {
                    Thread.sleep(timeslice);
                    System.out.println("DISTAPP : ("+workerName+") Time slice expired, interrupting task " + taskName);
                    workerThread.interrupt();
                } catch (InterruptedException e) {
                    // Timer thread was interrupted, task completed before time slice
                }
            });
            timerThread.start();

            // Execute the task - this may be interrupted
            try {
                dt.compute();
                // Task completed successfully
                timerThread.interrupt(); // Stop the timer
                Thread.interrupted(); // Clear interrupt status if it was set

                // Serialize our Task object back to a byte array!
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeObject(dt); oos.flush();
                taskSerial = bos.toByteArray();

                String taskResultPath = "/dist38/tasks/"+taskName+"/result";

                System.out.println("DISTAPP : ("+workerName+") I HAVE SUCCESSFULLY COMPLETED " + taskName);
                System.out.println("DISTAPP : ("+workerName+") Storing its result in " + taskResultPath);
                // Store it inside the result node.
                zk.create(taskResultPath, taskSerial, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

                // Also delete the assignment after, to trigger watch and let the manager know
                // that the worker that was working on this task is now finished
                zk.delete(assignmentPath, -1);

            } catch (InterruptedException ie) {
                // Task was interrupted - save current state and reschedule
                System.out.println("DISTAPP : ("+workerName+") Task " + taskName + " was interrupted, saving state for rescheduling");

                // Stop the timer thread
                timerThread.interrupt();

                // Serialize the task object with its current state (saved by the task's compute method)
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeObject(dt); oos.flush();
                taskSerial = bos.toByteArray();

                // Update the task znode with the current state so it can be resumed later
                String taskPath = "/dist38/tasks/"+taskName;
                Stat currentStat = zk.exists(taskPath, false);
                zk.setData(taskPath, taskSerial, currentStat.getVersion());

                System.out.println("DISTAPP : ("+workerName+") Saved intermediate state of " + taskName + " back to ZooKeeper");

                // IMPORTANT: Delete the assignment BEFORE exiting
                // This triggers the manager's watch on /dist38/assignments
                // The manager will detect this task needs rescheduling because:
                // 1. It's in tasksCompleted set
                // 2. But doesn't have a result znode
                // 3. And no longer has an active assignment
                zk.delete(assignmentPath, -1);

                System.out.println("DISTAPP : ("+workerName+") Deleted assignment, manager will reschedule " + taskName);
            }
        }

        catch(NodeExistsException nee){System.out.println(nee);}
        catch(KeeperException ke){System.out.println(ke);}
        catch(InterruptedException ie){System.out.println(ie);}
        catch(IOException io){System.out.println(io);}
        catch(ClassNotFoundException cne){System.out.println(cne);}
    }

    // Manager fetching task znodes...
    void getTasks()
    {
        zk.getChildren("/dist38/tasks", this, this, null);  
    }

    // Try to become the manager.
    void runForManager() throws UnknownHostException, KeeperException, InterruptedException
    {
        //Try to create an ephemeral node to be the manager, put the hostname and pid of this process as the data.
        // This is an example of Synchronous API invocation as the function waits for the execution and no callback is involved..
        zk.create("/dist38/manager", pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    public void process(WatchedEvent e)
    {
        //Get watcher notifications.

        //!! IMPORTANT !!
        // Do not perform any time consuming/waiting steps here
        //	including in other functions called from here.
        // 	Your will be essentially holding up ZK client library 
        //	thread and you will not get other notifications.
        //	Instead include another thread in your program logic that
        //   does the time consuming "work" and notify that thread from here.

        System.out.println("DISTAPP : Event received : " + e);
        if(e.getType() == Watcher.Event.EventType.None) // This seems to be the event type associated with connections.
        {
            // Once we are connected, do our intialization stuff.
            if(e.getPath() == null && e.getState() ==  Watcher.Event.KeeperState.SyncConnected && initialized == false) 
            {
                initialize();
                initialized = true;
            }
        }
        
        // Manager should be notified if any new znodes are added to tasks.
        if(e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist38/tasks"))
        {
            // There has been changes to the children of the node.
            // We are going to re-install the Watch as well as request for the list of the children.
            getTasks();
        }
        // Manager is notified if new znodes are added as workers
        else if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist38/workers"))
        {
            List<String> updatedChildren = new ArrayList<>();
            try {
                // Use 'this' to rewatch for new ephermal worker znodes
                updatedChildren = zk.getChildren("/dist38/workers", this);
                for(String child : updatedChildren) {
                    if(!workers.contains(child)) {
                        workers.add(child);
                        availableWorkers.add(child);
                    }
                }

                // Assign waiting tasks to newly available workers
                while(!tasksToDo.isEmpty() && !availableWorkers.isEmpty()) {
                    String nextTask = tasksToDo.peek();

                    // Check if task still exists and needs assignment
                    if (!taskExists(nextTask)) {
                        tasksToDo.poll();
                        tasksCompleted.remove(nextTask);
                        System.out.println("DISTAPP : Task " + nextTask + " no longer exists, removing from queue");
                        continue;
                    }

                    if (taskHasResult(nextTask)) {
                        tasksToDo.poll();
                        tasksCompleted.add(nextTask);
                        System.out.println("DISTAPP : Task " + nextTask + " already completed, removing from queue");
                        continue;
                    }

                    String nextWorker = availableWorkers.poll();
                    tasksToDo.poll();
                    tasksCompleted.add(nextTask);

                    String taskToAssign = nextTask;
                    String workerToUse = nextWorker;
                    System.out.println("DISTAPP : (Manager) New worker joined, assigning task " + taskToAssign + " to " + workerToUse);
                    new Thread(() -> assignWaitingTask(taskToAssign, workerToUse)).start();
                }
            } catch (Exception exception) {
                System.err.println("There was an issue getting children of /dist38/workers...");
            }
            
            System.out.println("DISTAPP : Current workers being managed: " + this.workers.toString());
            System.out.println("DISTAPP : Current available workers being managed: " + this.availableWorkers.toString());

        }
        // This clause is entered in the manager process to update available workers
        else if(workerName.equals("manager") && e.getType() == Watcher.Event.EventType.NodeChildrenChanged
        && e.getPath().equals("/dist38/assignments")) {
            try {
                List<String> assignments = zk.getChildren("/dist38/assignments", this);
                System.out.println("DISTAPP : (Manager) Current assignments: " + assignments.toString());

                // Get all current assignments to see which workers do not have an assignment
                // The ones with no assignment and which aren't already waiting
                // can be added to the availableWorkers queue

                // assignmentWorkers: the workers currently working on an assignment
                // availableWorkers: the workers currently NOT working on an assignment
                // workers: all workers being managed by the manager process

                List<String> assignmentWorkers = assignments.stream()
                   .map(a -> getAssignmentWorker(a))
                   .collect(Collectors.toList());

                for(String worker : new ArrayList<>(availableWorkers)) {
                    if(assignmentWorkers.contains(worker)) {
                        availableWorkers.remove(worker);
                        System.out.println("DISTAPP : (Manager) Removed " + worker + " from available workers (has active assignment)");
                    }
                }

                System.out.println("DISTAPP : Assignment workers: " + assignmentWorkers.toString());
                System.out.println("DISTAPP : Available workers: " + availableWorkers.toString());
                System.out.println("DISTAPP : Workers: " + workers.toString());

                // Check for interrupted tasks: tasks in tasksCompleted that don't have results
                // and don't have active assignments
                // These are tasks that were interrupted and need to be rescheduled
                List<String> activeTasks = assignments.stream()
                    .map(a -> getAssignmentTask(a))
                    .collect(Collectors.toList());

                List<String> tasksToReschedule = new ArrayList<>();
                for(String task : new ArrayList<>(tasksCompleted)) {
                    // A task needs rescheduling if:
                    // 1. It's marked as completed (was assigned)
                    // 2. It doesn't have a result znode (wasn't actually completed)
                    // 3. It doesn't have an active assignment (worker finished but was interrupted)
                    if (!taskHasResult(task) && !activeTasks.contains(task)) {
                        // Task was interrupted, remove from completed and reschedule
                        tasksCompleted.remove(task);
                        tasksToReschedule.add(task);
                        System.out.println("DISTAPP : (Manager) Task " + task + " was interrupted, rescheduling");
                    }
                }
                // Add interrupted tasks back to queue for rescheduling
                for(String task : tasksToReschedule) {
                    System.out.println("DISTAPP : Rescheduling task: " + task);
                    System.out.println("--------------------------------");
                    if (!tasksToDo.contains(task)) {
                        tasksToDo.add(task);
                    }
                }

                // Step 1: Identify newly available workers and add them to the queue
                for(String worker : workers) {
                    if(!assignmentWorkers.contains(worker) && !availableWorkers.contains(worker)) {
                        availableWorkers.add(worker);
                        System.out.println("DISTAPP : " + worker + " is ready for new tasks");
                    }
                }

                // Step 2: Assign waiting tasks to available workers
                // Keep assigning while we have both tasks and workers available
                while(!tasksToDo.isEmpty() && !availableWorkers.isEmpty()) {
                    String nextTask = tasksToDo.peek();

                    // Check if task still exists (might have been deleted by client after completion)
                    if (!taskExists(nextTask)) {
                        tasksToDo.poll();
                        tasksCompleted.remove(nextTask); // Not really completed, just gone
                        System.out.println("DISTAPP : Task " + nextTask + " no longer exists (deleted by client), removing from queue");
                        continue; // Check next task
                    }

                    // Check if task already has result (might have been completed by another worker)
                    if (taskHasResult(nextTask)) {
                        tasksToDo.poll();
                        tasksCompleted.add(nextTask);
                        System.out.println("DISTAPP : Task " + nextTask + " in queue already completed, removing from queue");
                        continue; // Check next task
                    }

                    // Task is valid and needs to be assigned
                    String nextWorker = availableWorkers.poll();
                    tasksToDo.poll();
                    tasksCompleted.add(nextTask);

                    // Create assignment in a separate thread to avoid blocking
                    String taskToAssign = nextTask;
                    String workerToUse = nextWorker;
                    System.out.println("DISTAPP : (Manager) Assigning waiting task " + taskToAssign + " to " + workerToUse);
                    new Thread(() -> assignWaitingTask(taskToAssign, workerToUse)).start();
                }

                System.out.println("DISTAPP : (Manager) Available workers: " + availableWorkers.toString());
            } catch (Exception ex) {
                System.err.println("DISTAPP : (Manager) Exception in assignments handler: " + ex);
            }

        }
        // This clause is entered in a worker process when a worker detects a new task has been assigned to it
        else if(e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals("/dist38/assignments")) {
            try {
                List<String> assignments = zk.getChildren("/dist38/assignments", this);
                for(String a : assignments) {
                    // If the new task was assigned to THIS worker, it needs to process the task
                    if(this.getAssignmentWorker(a).equals(workerName) && !processedAssignments.contains(a)) {
                        System.out.println("DISTAPP : ("+workerName+") I HAVE A NEW TASK TO DO");
                        // Mark as processed before starting thread to prevent race condition
                        processedAssignments.add(a);
                        // Compute task in new thread of worker DistProcess
                        new Thread(() -> {
                            computeTaskResult(a);
                            processedAssignments.remove(a); // Clean up after completion
                        }).start();
                    }
                }
            } catch (KeeperException keeperException) {
                System.out.println("Something went wrong when checking if worker was assigned task");
            } catch(InterruptedException interruptedException) {
                System.out.println("Interrupt while checking if worker was assigned task");
            }

        }
    }

    //Asynchronous callback that is invoked by the zk.getChildren request.
    public void processResult(int rc, String path, Object ctx, List<String> children)
    {

        //!! IMPORTANT !!
        // Do not perform any time consuming/waiting steps here
        //	including in other functions called from here.
        // 	Your will be essentially holding up ZK client library 
        //	thread and you will not get other notifications.
        //	Instead include another thread in your program logic that
        //   does the time consuming "work" and notify that thread from here.

        // This logic is for manager !!
        //Every time a new task znode is created by the client, this will be invoked.

        // TODO: Filter out and go over only the newly created task znodes.
        //		Also have a mechanism to assign these tasks to a "Worker" process.
        //		The worker must invoke the "compute" function of the Task send by the client.
        //What to do if you do not have a free worker process?
        System.out.println("DISTAPP : processResult : " + rc + ":" + path + ":" + ctx);
        for(String c: children)
        {
            System.out.println(c);
            // Do some task filtering logic here to get the newly created task
            // Skip tasks that are already completed (have result nodes) or are already assigned
            // Also skip tasks that no longer exist (deleted by client)
            if(!tasksCompleted.contains(c) && !taskHasResult(c) && taskExists(c)) {
                // Note that poll() does not actually poll for items in the queue,
                // it simply returns the head if it exists and otherwise returns null
                // remove () is an alternative but throws exception instead
                System.out.println("DISTAPP : Available workers before poll: " + availableWorkers.size() + " - " + availableWorkers.toString());
                String availableWorker = availableWorkers.poll();
                System.out.println("DISTAPP : Polled worker: " + availableWorker);
                // If there is a worker available, assign it to task
                if(availableWorker != null) {
                    System.out.println("DISTAPP : Assigning " + c + " to " + availableWorker);
                    // Get task data asynchronously
                    zk.getData("/dist38/tasks/" + c, false, new DataCallback() {
                        @Override
                        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                            if (rc == 0) {
                                System.out.println("DISTAPP : Got task data for " + path);
                                try {
                                    // Check again if task still exists (might have been deleted by client)
                                    String taskName = path.substring(path.lastIndexOf('/') + 1);
                                    if (!taskExists(taskName)) {
                                        System.out.println("DISTAPP : Task " + taskName + " no longer exists (deleted by client), skipping assignment");
                                        // Return worker to available pool
                                        availableWorkers.add(availableWorker);
                                        return;
                                    }

                                    // Check if task has result (might have been completed while we were getting data)
                                    if (taskHasResult(taskName)) {
                                        System.out.println("DISTAPP : Task " + taskName + " already completed, skipping assignment");
                                        // Return worker to available pool
                                        availableWorkers.add(availableWorker);
                                        return;
                                    }

                                    String newAssignment = "/dist38/assignments/" + availableWorker + "-"  + c;
                                    System.out.println("DISTAPP : Creating new assignment: " + newAssignment);
                                    tasksCompleted.add(c);
                                    zk.create(newAssignment, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                                } catch(NodeExistsException nee) {
                                    System.out.println(availableWorker + " already in use");
                                    // Return worker to available pool
                                    availableWorkers.add(availableWorker);
                                } catch(Exception ex) {
                                    System.out.println("Non-NodeExistsException happened when trying to create assignment");
                                    // Return worker to available pool
                                    availableWorkers.add(availableWorker);
                                }
                            }
                            else {
                                System.err.println("Failed to get data from " + path + ", rc=" + rc);
                                // Return worker to available pool
                                availableWorkers.add(availableWorker);
                            }
                        }
                    }, null);                    
                }
                else {
                    // There was no worker available: put task in a queue to be done when
                    // one does become available
                    System.out.println("DISTAPP : Enqueuing " + c + " for later...");
                    tasksToDo.add(c);
                }
            } 
        }
    }

    public static void main(String args[]) throws Exception
    {
        //Create a new process
        //Read the ZooKeeper ensemble information from the environment variable.
        DistProcess dt = new DistProcess(System.getenv("ZKSERVER"));
        dt.startProcess();

        // Add shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down gracefully...");
            try {
                if (dt.zk != null) {
                    dt.zk.close();
                }
                System.out.println("ZooKeeper connection closed");
            } catch (InterruptedException e) {
                System.err.println("Error closing ZooKeeper: " + e);
            }
        }));

        // Keep the process running indefinitely
        synchronized(dt) {
            try {
                dt.wait(); // Wait forever
            } catch (InterruptedException ie) {
                System.out.println("Process interrupted, shutting down...");
            }
        }
    }
}