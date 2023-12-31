import java.util.*;

// Workflow scheduling class
class WorkflowScheduling {

    // Weighted graph to represent workflow
    static class WeightedGraph {
        // Map of vertices by unique id
        Map<Character, Vertex> vertices = new HashMap<>();

        // Add vertex with id and duration
        void addVertex(char id, int duration) {
            vertices.put(id, new Vertex(id, duration));
        }

        // Add weighted edge between vertices
        void addEdge(char u, char v, int weight) {
            vertices.get(u).outputs.add(new Edge(vertices.get(v), weight));
        }

        // Get underlying vertices map
        Map<Character, Vertex> getVertices() {
            return vertices;
        }

        // Vertex representing a job
        static class Vertex {
            // Job id - Vertex name
            char id;
            // Job duration for a particular vertex
            int duration;

            // Completion time after scheduling
            int completionTime = -1;
            // Outgoing edges to dependents
            List<Edge> outputs = new ArrayList<>();

            // Constructor
            Vertex(char id, int duration) {
                this.id = id;
                this.duration = duration;
            }

            // Get the time of a vertex when it completes its job.
            public int getCompletionTime(){

                return completionTime;
            }


            // String representation of an instant
            @Override
            public String toString() {
                return String.valueOf(id);
            }
        }

        // Directed edge representing dependencies
        static class Edge {
            // Destination vertex
            Vertex dest;

            // Dependency weight as data transfer time
            int weight;

            // Constructor
            Edge(Vertex dest, int weight) {
                this.dest = dest;
                this.weight = weight;
            }
            @Override
            public String toString() {
                return "(" + dest.id + ", " + weight + ")";
            }
        }
    }

    // Topologically sort graph vertices
    public static List<Character> topologicalSort(WeightedGraph graph) {
        List<Character> result = new ArrayList<>();
        Queue<WeightedGraph.Vertex> queue = new LinkedList<>();

        // Calculate in-degrees for each vertex
        int[] inDegrees = new int[graph.getVertices().size()];
        for (WeightedGraph.Vertex v : graph.getVertices().values()) {
            for (WeightedGraph.Edge e : v.outputs) {
                inDegrees[e.dest.id - 'A']++;
            }
        }

        // Enqueue vertices with in-degree 0
        for (WeightedGraph.Vertex v : graph.getVertices().values()) {
            if (inDegrees[v.id - 'A'] == 0) {
                queue.offer(v);
            }
        }

        // BFS traversal to produce topological order
        while (!queue.isEmpty()) {
            WeightedGraph.Vertex v = queue.poll();
            result.add(v.id);

            for (WeightedGraph.Edge e : v.outputs) {
                // Decrease in-degree of the destination vertex
                inDegrees[e.dest.id - 'A']--;

                // If in-degree becomes 0, enqueue the destination vertex
                if (inDegrees[e.dest.id - 'A'] == 0) {
                    queue.offer(e.dest);
                }
            }
        }

        return result;
    }

    // Schedule jobs on machines
    public static int schedule(WeightedGraph graph, int numMachines) {

        // No machine for jobs scheduling
        if(numMachines <= 0)
            return 0;

        // Schedule jobs on a single machine
        if (numMachines == 1) {
            int totalDuration = 0;

            // Calculate sum of all vertices' durations
            for (WorkflowScheduling.WeightedGraph.Vertex vertex : graph.getVertices().values()) {
                totalDuration += vertex.duration;
            }

            // Calculate sum of all edges' weights
            int totalEdgeWeights = 0;
            for (WorkflowScheduling.WeightedGraph.Vertex vertex : graph.getVertices().values()) {
                for (WorkflowScheduling.WeightedGraph.Edge edge : vertex.outputs) {
                    totalEdgeWeights += edge.weight;
                }
            }

            int totalWeight = totalDuration + totalEdgeWeights;

            // Return the total weight
            return totalWeight;
        }

        // Get topological order of jobs
        List<Character> order = topologicalSort(graph);

        // Print the Topological order
        System.out.println("\nTopological sort (Job execution order): "+order);
        System.out.println();

        // Track scheduled vertices for each machine
        List<List<Character>> scheduledVertices = new ArrayList<>();
        for (int i = 0; i < numMachines; i++) {
            scheduledVertices.add(new ArrayList<>());
        }

        // Initialize machine finish time
        int[] machineFinishTime = new int[numMachines];

        // Schedule jobs one by one
        for (char jobId : order) {

            // Get job vertex
            WeightedGraph.Vertex job = graph.getVertices().get(jobId);

            // Find the predecessors of job vertex
            List<WeightedGraph.Vertex> inputs = getPredecessors(graph, job);
            System.out.println("\nPredecessors of "+jobId+" are: "+inputs);
            System.out.println();

            // Find the machine with earliest finish time
            int earliestMachine = findEarliestMachine(machineFinishTime);
            int maxDependencyFinishTime = 0;

            // Update finish time based on dependencies
            for (WeightedGraph.Vertex in : inputs) {

                WeightedGraph.Edge edge = getEdge(graph, in, job);

                int dependencyFinishTime = Math.max(in.getCompletionTime(),machineFinishTime[earliestMachine])+edge.weight;
                System.out.println("Dependency "+ in +" Finish Time: "+ dependencyFinishTime);
                maxDependencyFinishTime = Math.max(maxDependencyFinishTime, dependencyFinishTime);
                System.out.println("Maximum Dependency Finish Time: "+maxDependencyFinishTime);
            }

            // No predecessors
            if(inputs.size()==0){

                // Update finish time with job duration
                machineFinishTime[earliestMachine] +=  job.duration;

                // Set job completion time
                job.completionTime = machineFinishTime[earliestMachine];
            }
            else {

                // Update finish time with job duration
                machineFinishTime[earliestMachine] = Math.max(maxDependencyFinishTime,machineFinishTime[earliestMachine]) + job.duration;

                // Set job completion time
                job.completionTime = maxDependencyFinishTime + job.duration;

            }

            // Track scheduled vertices for the machine
            scheduledVertices.get(earliestMachine).add(job.id);
//            System.out.println(scheduledVertices);
            System.out.print("\nMachines Finish time: [ ");
            for(int m=0;m<numMachines;m++){
                System.out.print(machineFinishTime[m]+" ");
            }
            System.out.println("]");
            System.out.println("------");
        }

        // Print vertices scheduled on each machine
        for (int i = 0; i < numMachines; i++) {
            System.out.println("Machine " + (i + 1) + " scheduled vertices: " + scheduledVertices.get(i));
        }


        // Get overall makespan
        int makespan = Arrays.stream(machineFinishTime).max().orElse(0);
        return makespan;
    }

    // Find the machine with the earliest finish time
    private static int findEarliestMachine(int[] machineFinishTime) {
        int earliestMachine = 0;
        for (int i = 1; i < machineFinishTime.length; i++) {
            if (machineFinishTime[i] < machineFinishTime[earliestMachine]) {
                earliestMachine = i;
            }
        }
        return earliestMachine;
    }

    // Get all the input vertices of a particular vertex
    public static List<WeightedGraph.Vertex> getPredecessors(WeightedGraph graph, WeightedGraph.Vertex vertex) {
        List<WeightedGraph.Vertex> predecessors = new ArrayList<>();
        for (WeightedGraph.Vertex v : graph.getVertices().values()) {
            for (WeightedGraph.Edge edge : v.outputs) {
                if (edge.dest == vertex) {
                    predecessors.add(v);
                }
            }
        }
        return predecessors;
    }

    // Get edge between two vertices
    public static WeightedGraph.Edge getEdge(WeightedGraph graph, WeightedGraph.Vertex source, WeightedGraph.Vertex destination) {
        for (WeightedGraph.Edge edge : source.outputs) {
            if (edge.dest == destination) {
                return edge;
            }
        }
        return null;
    }


    // Main driver method
    public static void main(String[] args) {

        // Create workflow graph
        WeightedGraph graph = new WeightedGraph();

        // Adding vertices and edges - Add jobs and dependencies

        // Example-1
        graph.addVertex('A', 5);
        graph.addVertex('B', 3);
        graph.addVertex('C', 8);
        graph.addVertex('D', 4);
        graph.addVertex('E', 2);
        graph.addVertex('F', 1);
        graph.addVertex('G', 7);
        graph.addVertex('H', 3);
        graph.addEdge('A', 'D', 2);
        graph.addEdge('B', 'D', 1);
        graph.addEdge('C', 'D', 5);
        graph.addEdge('D', 'E', 3);
        graph.addEdge('D', 'F', 4);
        graph.addEdge('E', 'G', 1);
        graph.addEdge('F', 'G', 2);
        graph.addEdge('G', 'H', 2);

        // Example -2
//        graph.addVertex('A', 2);
//        graph.addVertex('B', 3);
//        graph.addVertex('C', 4);
//        graph.addVertex('D', 9);
//        graph.addVertex('E', 7);
//        graph.addVertex('F', 3);
//        graph.addVertex('G', 2);
//        graph.addVertex('H', 3);
//        graph.addVertex('I', 5);
//        graph.addVertex('J', 7);
//        graph.addEdge('A', 'D', 3);
//        graph.addEdge('B', 'D', 2);
//        graph.addEdge('B', 'E', 3);
//        graph.addEdge('C', 'E', 2);
//        graph.addEdge('D', 'F', 1);
//        graph.addEdge('E', 'G', 5);
//        graph.addEdge('E', 'H', 2);
//        graph.addEdge('F', 'I', 3);
//        graph.addEdge('G', 'J', 3);
//        graph.addEdge('H', 'I', 3);
//        graph.addEdge('H', 'J', 3);

        //Example-3
//        graph.addVertex('A', 3);
//        graph.addVertex('B', 3);
//        graph.addVertex('C', 3);
//        graph.addVertex('D', 3);
//        graph.addVertex('E', 3);
//        graph.addVertex('F', 3);
//        graph.addVertex('G', 3);
//        graph.addVertex('H', 3);
//        graph.addVertex('I', 3);
//        graph.addVertex('J', 3);
//        graph.addVertex('K', 3);
//        graph.addVertex('L', 3);
//        graph.addVertex('M', 3);
//        graph.addEdge('C', 'A', 2);
//        graph.addEdge('C', 'B', 2);
//        graph.addEdge('D', 'B', 2);
//        graph.addEdge('D', 'G', 2);
//        graph.addEdge('D', 'H', 2);
//        graph.addEdge('E', 'A', 2);
//        graph.addEdge('E', 'D', 2);
//        graph.addEdge('E', 'F', 2);
//        graph.addEdge('F', 'K', 2);
//        graph.addEdge('F', 'J', 2);
//        graph.addEdge('G', 'I', 2);
//        graph.addEdge('H', 'I', 2);
//        graph.addEdge('J', 'I', 2);
//        graph.addEdge('J', 'L', 2);
//        graph.addEdge('J', 'M', 2);
//        graph.addEdge('K', 'J', 2);

        // Set the number of machines
        int numMachines = 2;
        int makespan = schedule(graph, numMachines);

        if(numMachines>1) {
            // Print vertices with completion times
            System.out.println("\nJob Completion Times:");
            for (WeightedGraph.Vertex vertex : graph.getVertices().values()) {
                System.out.println(vertex.id + ": " + vertex.completionTime);
            }
        }

        // Print results
        System.out.println("\nMinimum execution time of entire workflow: " + makespan);

    }
}
