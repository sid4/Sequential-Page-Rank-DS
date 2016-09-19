import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import mpi.MPI;


public class PageRankMPI implements PageRank {
    private static final int ROOT_NODE_ID=0;
	// adjacency matrix read from file
    private ConcurrentMap<Integer, List<Integer>> adjMatrix;
    private ConcurrentMap<Integer, List<Integer>> indegreeMatrix;
    private ConcurrentMap<Integer,Integer> outDegreeCount;
    // input file name
    private String inputFile = "";
    // output file name
    private String outputFile = "";
    // number of iterations
    private int iterations = 10;
    // damping factor
    private double df = 0.85;
    // number of URLs
    private int size = 0;
    // calculating rank values
    private ConcurrentMap<Integer, WrappedDouble> rankValues; 
    private int processID;
    private int nodes;
    public PageRankMPI(int processID, int nodes){
    	this.nodes=nodes;
    	this.processID=processID;
    	adjMatrix = new ConcurrentHashMap<>();
    	rankValues= new ConcurrentHashMap<Integer, WrappedDouble>();
    }
	private static class WrappedDouble implements Comparable<WrappedDouble>{
		private double val;
		WrappedDouble(double val){
			this.val=val;
		}
		public WrappedDouble  add(double val1){
			val+=val1;
			return this;
		}
		public WrappedDouble  multiply(double val1){
			val*=val1;
			return this;
		}
		
		public double getVal(){
			return val;
		}
		@Override
		public int compareTo(WrappedDouble other){
			return Double.compare(val, other.getVal());
		}
	}

   
    
    /**
     * Parse the command line arguments and update the instance variables. Command line arguments are of the form
     * <input_file_name> <output_file_name> <num_iters> <damp_factor>
     *
     * @param args arguments
     */
    public void parseArgs(String[] args) {
		//only parse args if its a root node ID
    	if (processID == ROOT_NODE_ID) {
			if (args.length < 4) {
				System.err
						.println("Please invocate the program with following arguments [input file name] [output file name] [iteration count] [damping factor]");
				System.exit(-1);
			} else {
				inputFile = args[0];
				outputFile = args[1];
				iterations = Integer.parseInt(args[2]);
				df = Double.parseDouble(args[3]);
			}
		}
    }
	 /**
     * Read the input from the file and populate the adjacency matrix
     *
     * The input is of type
     *
     0
     1 2
     2 1
     3 0 1
     4 1 3 5
     5 1 4
     6 1 4
     7 1 4
     8 1 4
     9 4
     10 4
     * The first value in each line is a URL. Each value after the first value is the URLs referred by the first URL.
     * For example the page represented by the 0 URL doesn't refer any other URL. Page
     * represented by 1 refer the URL 2.
     *
     * @throws java.io.IOException if an error occurs
     */
	public void loadInput() throws IOException {
		if (processID == ROOT_NODE_ID) {
			Path inputFilePath = Paths.get(inputFile);
			// read all lines
			Files.lines(inputFilePath)
					.parallel()
					// split each line
					.map(line -> line.trim().split(" "))
					// populate adjacency matrix with given splitted lines
					.forEach(
							(splittedLine) -> {
								adjMatrix.put(
										Integer.valueOf(Integer
												.parseInt(splittedLine[0])),
										Arrays.stream(splittedLine)
												.skip(1)
												.parallel()
												.map(el -> Integer.parseInt(el))
												.collect(Collectors.toList()));
							});
			
			// initialize size
			size = adjMatrix.size();
			//populate indegree matrix
			adjMatrix.entrySet().forEach(entry->{
				entry.getValue().forEach(node->{
					if(indegreeMatrix.containsKey(node)){
						indegreeMatrix.get(node).add(entry.getKey());
					}
					else{
						List<Integer> inbound=new LinkedList<>();
						inbound.add(entry.getKey());
						indegreeMatrix.put(node, inbound);
					}
				});
			});
			//populate outdegree counts and handle dangling nodes by setting outdegree count to size and updating indegreeMatrix
			outDegreeCount=adjMatrix.entrySet().parallelStream().collect((Collectors.toConcurrentMap(e->e.getKey(),e->{int outdegree; if(e.getValue().size()==0){outdegree=size;indegreeMatrix.values().forEach(v->v.add(e.getKey()));}else{outdegree=e.getValue().size();} return outdegree; })));
		}
	}
    /**
     * Partitions the indegree matrix and outdegree count data to to all the peers
     */
	private void initializeAllNodeData(){
		if (processID == ROOT_NODE_ID) {
    		//SEND indegree matrix and outdegree count to all the peers
        	//partition size of data to be distributed
    		int partitionSize=size/nodes;
    		IntStream
    		.rangeClosed(2, nodes)
        	.parallel()
    		.forEach(i->{
        		ConcurrentMap<Integer, List<Integer>> indegreeMatrixBuffer[] = new ConcurrentHashMap[1];
        		ConcurrentMap<Integer,Integer> outDegreeCountBuffer[] = new ConcurrentHashMap[1];
        		//populate buffer with corresponding indegree partitioned data to each node/process
        		indegreeMatrixBuffer[0]=indegreeMatrix.entrySet().stream().filter(entry->(entry.getKey()>(i-1)*partitionSize) && ((i==nodes)?true:(entry.getKey()<=i*partitionSize))).collect(Collectors.toConcurrentMap(t -> t.getKey(), t -> t.getValue()));
        		//populate buffer with corresponding outdegreeCount partitioned data to each node/process
        		//outDegreeCountBuffer[0]=outDegreeCount.entrySet().stream().filter(entry->(entry.getKey()>(i-1)*partitionSize) && ((i==nodes)?true:(entry.getKey()<=i*partitionSize))).collect(Collectors.toConcurrentMap(t -> t.getKey(), t -> t.getValue()));
        		outDegreeCountBuffer[0]=outDegreeCount.entrySet().stream().filter(entry-> adjMatrix.get(entry.getKey()).stream().anyMatch(out->indegreeMatrixBuffer[0].containsKey(out))).collect(Collectors.toConcurrentMap(t -> t.getKey(), t -> t.getValue()));
        		//send corresponding indegree partitioned data to each node/process
        		MPI.COMM_WORLD.Send(indegreeMatrixBuffer, 0, 1, MPI.OBJECT, i-1, 1);	
        		//send corresponding outdegreeCount partitioned data to each node/process
        		MPI.COMM_WORLD.Send(outDegreeCountBuffer, 0, 1, MPI.OBJECT, i-1, 2);	
    		});
    		//partition data for self
    		indegreeMatrix=indegreeMatrix.entrySet().stream().filter(entry->(entry.getKey()<=partitionSize)).collect(Collectors.toConcurrentMap(t -> t.getKey(), t -> t.getValue()));
    		//outDegreeCount=outDegreeCount.entrySet().stream().filter(entry->(entry.getKey()<=partitionSize)).collect(Collectors.toConcurrentMap(t -> t.getKey(), t -> t.getValue()));
    		outDegreeCount=outDegreeCount.entrySet().stream().filter(entry-> adjMatrix.get(entry.getKey()).stream().anyMatch(out->indegreeMatrix.containsKey(out))).collect(Collectors.toConcurrentMap(t -> t.getKey(), t -> t.getValue()));
    	}
    	else{
    		//RECEIVE indegree matrix and outdegree count from root node
    		ConcurrentMap<Integer, List<Integer>> indegreeMatrixBuffer[] = new ConcurrentHashMap[1];
    		ConcurrentMap<Integer,Integer> outDegreeCountBuffer[] = new ConcurrentHashMap[1];
    		MPI.COMM_WORLD.Recv(indegreeMatrixBuffer, 0, 1, MPI.OBJECT, 0, 1);
    		MPI.COMM_WORLD.Recv(outDegreeCountBuffer, 0, 1, MPI.OBJECT, 0, 2);
    		indegreeMatrix=indegreeMatrixBuffer[0];
    		outDegreeCount=outDegreeCountBuffer[0];
    	}
	}
	
	/**
	 * Distributes the current global page rank
	 */
	private void distributeGlobalPageRank() {
		final ConcurrentMap<Integer, WrappedDouble> rankValuesBuffer[] = new ConcurrentHashMap[1];
		rankValuesBuffer[0] = rankValues;
		MPI.COMM_WORLD.Bcast(rankValuesBuffer, 0, 1, MPI.OBJECT, 0);
		if(processID != ROOT_NODE_ID){
			rankValues=(ConcurrentMap<Integer, WrappedDouble>)rankValuesBuffer[0];
		}
	}
	
    /**
     * Do fixed number of iterations and calculate the page rank values. You may keep the
     * intermediate page rank values in a hash table.
     */
    public void calculatePageRank() {
    	initializeAllNodeData();
    	//completed intialization of all nodes with indegreeMatrix and outDegreeCount
    	//run page rank on each node/process for given no. of iterations
    	//iteration 0: initialize the page rank of all the nodes to 1/n
    	adjMatrix.keySet().parallelStream().forEach(key->rankValues.put(key,new WrappedDouble(1.0/size)));
    	final double DF_FACTOR=(1-df)/rankValues.size();
    	IntStream
    	.rangeClosed(1, iterations)
    	.forEach(i->{
    		//using inbound approach for calculating ranks
    		distributeGlobalPageRank();
    		ConcurrentMap<Integer, WrappedDouble> rankValuesIntermmediate=indegreeMatrix.entrySet().parallelStream().collect(Collectors.toConcurrentMap(t -> t.getKey(), t->{return new WrappedDouble(t.getValue().parallelStream().mapToDouble(p->p).reduce(0,(a,b)->{return (rankValues.get((int) a).getVal()/outDegreeCount.get((int) a))+(rankValues.get((int) b).getVal()/outDegreeCount.get((int) b));})).multiply(df).add(DF_FACTOR);}));
    		if (processID == ROOT_NODE_ID) {
    			IntStream
    	    	.range(1,nodes).forEach(j->{
    	    		//combine all the ranks at root node
    	    		final ConcurrentMap<Integer, WrappedDouble> rankValuesBuffer[] = new ConcurrentHashMap[1];
    	    		MPI.COMM_WORLD.Recv(rankValuesBuffer, 0, 1, MPI.OBJECT, j, 3);
    	    		rankValuesIntermmediate.putAll(rankValuesBuffer[0]);
    	    	});
    			rankValues=rankValuesIntermmediate;
    		}
    		else{
    			final ConcurrentMap<Integer, WrappedDouble> rankValuesBuffer[] = new ConcurrentHashMap[1];
    			rankValuesBuffer[0] = rankValuesIntermmediate;
    			//send local ranks to root node
    			MPI.COMM_WORLD.Send(rankValuesBuffer, 0, 1, MPI.OBJECT, 0, 3);
    		}
    		
    	});
    	    	
//    	if (processID == ROOT_NODE_ID) {
//    		/*
//    		  protected void send(java.lang.Object buf,
//        int offset,
//        int count,
//        Datatype datatype,
//        int dest,
//        int tag,
//        boolean pt2pt)
//             throws MPIException
//    		 */
//    		ConcurrentMap<Integer, AtomicDouble> globalRankbuffer[] = (ConcurrentMap<Integer, AtomicDouble> buffer[])new ConcurrentHashMap[1];
//    		globalRankbuffer[0]=rankValues;
//    		//send current rank values to all the nodes
//    		IntStream
//        	.rangeClosed(1, nodes-1)
//        	.forEach(i->
//    		MPI.COMM_WORLD.Send(values, 0, 1, MPI.INT, i, 1));
//    		
//    	}
//    	else{
//    		/*
//    		protected Status recv(java.lang.Object buf,
//    		          int offset,
//    		          int count,
//    		          Datatype datatype,
//    		          int source,
//    		          int tag,
//    		          boolean pt2pt)
//    		          */
//    		ConcurrentMap<Integer, AtomicDouble> globalRankbuffer[] = (ConcurrentMap<Integer, AtomicDouble>)new ConcurrentHashMap[1];
//    		MPI.COMM_WORLD.Recv(globalRankbuffer, 0, 1, MPI.OBJECT, 0, 1);
//    		rankValues=globalRankbuffer[0];
//
//    	}
    	//iteration 0: initialize the page rank of all the nodes to 1/n
//    	adjMatrix.keySet().parallelStream().forEach(key->rankValues.put(key,new AtomicDouble(1.0/size)));
//    	
//    	IntStream
//    	.rangeClosed(1, iterations)
//    	.forEach(i->{
//    		//using outbound approach for calculating ranks
//    		ConcurrentMap<Integer, AtomicDouble> rankValuesIntermmediate=adjMatrix.keySet().parallelStream().collect(Collectors.toConcurrentMap(Function.identity(), t->new AtomicDouble(0.0))); 		
//    		adjMatrix.entrySet().parallelStream()
//    		.forEach(entry->{
//    			//handle dangling node
//    			if(entry.getValue().size()==0){
//    				rankValuesIntermmediate.keySet().stream().parallel()
//    				.forEach(node->rankValuesIntermmediate.get(node).add(rankValues.get(entry.getKey()).getVal()/size));	
//    			}
//    			else{
//    				entry.getValue().stream().parallel().forEach(outBoundNode->
//    					rankValuesIntermmediate
//    					.get(outBoundNode)
//    					.add(rankValues.get(entry.getKey()).getVal()/entry.getValue().size()));
//    			}
//    			});
//    		final double DF_FACTOR=(1-df)/size;
//    		//factor in the damping factor
//    		rankValuesIntermmediate.entrySet()
//    		.parallelStream()
//    		.forEach(entry->entry.getValue().multiply(df).add(DF_FACTOR));
//    		rankValues=rankValuesIntermmediate;
//    	});
    	    	
    	
    }
	 /**
     * Print the pagerank values. Before printing you should sort them according to decreasing order.
     * Print all the values to the output file. Print only the first 10 values to console.
     *
     * @throws IOException if an error occurs
     */
    public void printValues() throws IOException {
		if (processID == ROOT_NODE_ID) {
			// Map<Double,Integer> results=new
			// TreeMap<>(Collections.reverseOrder());
			StringBuilder output = new StringBuilder();
			rankValues
					.entrySet()
					.parallelStream()
					// sort by page rank
					.sorted(Map.Entry.comparingByValue(Collections
							.reverseOrder()))
					// limit to top 10
					// .limit(10)
					// create output
					.forEachOrdered(
							entry -> output.append("Page: " + entry.getKey()
									+ " : Rank: " + entry.getValue().getVal()
									+ "\n"));
			String outputResult = output.toString();
			System.out.println("No of iterations:"
					+ iterations
					+ "\n"
					+ outputResult.substring(0,
							getNthOccurenceOf(outputResult, "\n", 10)));
			// writing output to file
			Files.write(Paths.get(outputFile), outputResult.getBytes());
		}
    }
	public static void main(String args[]) throws IOException{
	        long start=System.currentTimeMillis();
	        MPI.Init(args);
	        PageRank mpiPR = new PageRankMPI(MPI.COMM_WORLD.Rank(),MPI.COMM_WORLD.Size());
	        mpiPR.parseArgs(args);
	        mpiPR.loadInput();
	        mpiPR.calculatePageRank();
	        MPI.Finalize();
	        mpiPR.printValues();
	        System.out.println("Sequential Execution Completed in:"+(System.currentTimeMillis()-start)+"ms");
	        
	}
	 private int getNthOccurenceOf(String text,String pattern,int n){
	    	int index=text.indexOf(pattern,0);
	    	
	    	while(--n>0 && index!=-1){
	    		index=text.indexOf(pattern,index+1);
	    	}
	    	if(index==-1){
	    		index=text.length()-1;
	    	}
	    	return index;
	    }
}
