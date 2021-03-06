import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import mpi.MPI;


public class PageRankMPIDebug implements PageRank {
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
    public PageRankMPIDebug(int processID, int nodes){
    	this.nodes=nodes;
    	this.processID=processID;
    	adjMatrix = new ConcurrentHashMap<>();
    	rankValues= new ConcurrentHashMap<Integer, WrappedDouble>();
    	indegreeMatrix= new ConcurrentHashMap<>();
    }
	private static class WrappedDouble implements Comparable<WrappedDouble>,Serializable{
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
			if (args.length < 7) {
				System.err
						.println("Please invocate the program with following arguments [input file name] [output file name] [iteration count] [damping factor]");
				System.exit(-1);
			} else {
				Arrays.stream(args).forEach(arg->System.out.println(arg));
				inputFile = args[3];
				outputFile = args[4];
				iterations = Integer.parseInt(args[5]);
				df = Double.parseDouble(args[6]);
			}
		}
    	else{
    		iterations = Integer.parseInt(args[5]);
			df = Double.parseDouble(args[6]);
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
					//.parallel()
					
			        .filter(line->!line.isEmpty() && !line.equals("") && !line.trim().equals("\n"))
					// split each line
			        .map(line -> line.trim().split(" "))
					// populate adjacency matrix with given splitted lines
					.forEach(
							(splittedLine) -> {
								
								Integer node=Integer.valueOf(Integer
										.parseInt(splittedLine[0]));
								adjMatrix.put(
										node,
										Arrays.stream(splittedLine)
												.skip(1)
												.parallel()
												.map(el -> Integer.parseInt(el))
												.collect(Collectors.toList()));
								if(!indegreeMatrix.containsKey(node)){
									if(node==null){
										System.out.println("adjMatrix Key null");
										System.exit(-1);
									}
									indegreeMatrix.put(node, new ArrayList<>());
								}
							});
			
			// initialize size
			size = adjMatrix.size();
			System.out.println(size);
			//populate indegree matrix
			adjMatrix.entrySet().forEach(entry->{
				entry.getValue().forEach(node->{
				//	if(indegreeMatrix.containsKey(node)){
					if(entry.getKey()==null){
						System.out.println(entry.getKey());
						System.exit(-1);
					}
						indegreeMatrix.get(node).add(entry.getKey());
				//	}
//					else{
//						List<Integer> inbound=new LinkedList<>();
//						inbound.add(entry.getKey());
//						indegreeMatrix.put(node, inbound);
					//}
				});
			});
//			for(Map.Entry<Integer,List<Integer>> entry:adjMatrix.entrySet()){
//				for(Integer val:entry.getValue()){
//					if(indegreeMatrix.containsKey(val)){
//						indegreeMatrix.get(val).add(entry.getKey());
//					}
//					else{
//						List<Integer> inbound=new LinkedList<>();
//						inbound.add(entry.getKey());
//						indegreeMatrix.put(val, inbound);
//					}
//				}
//			}
//			if(adjMatrix.containsKey(null)){
//				System.out.println("Million dollar qn");
//				System.exit(-1);
//			}
			System.out.println("indegree0:"+indegreeMatrix.size());
			indegreeMatrix.forEach((k,v)->{if(k==null||v==null||v.contains(null)){System.out.println("k:"+k+"v:"+v);System.exit(-1);} else System.out.println(k+":"+Arrays.toString(v.toArray()));});

			//populate outdegree counts and handle dangling nodes by setting outdegree count to size and updating indegreeMatrix
			outDegreeCount = new ConcurrentHashMap<Integer, List<Integer>>(
					adjMatrix)
					.entrySet()
					//do not parellilize causes problems
					.stream()
					.collect(
							(Collectors.toConcurrentMap(
									e -> e.getKey(),
									e -> {
										int outdegree;
										if(e.getKey()==null){
											System.out.println("BUG BUG");
											System.exit(-1);
										}
										if (e.getValue().size() == 0) {
											outdegree = size;
											indegreeMatrix.values().forEach(
													v -> v.add(e.getKey()));
											adjMatrix.get(e.getKey()).addAll(
													adjMatrix.keySet());
										} else {
											outdegree = e.getValue().size();
										}
										return outdegree;
									})));
			System.out.println("outdegree");
			outDegreeCount.forEach((k,v)->{if(k==null||v==null){System.out.println("k:"+k+"v:"+v);System.exit(-1);}else System.out.println(k+":"+v);});
			System.out.println("indegree1:"+indegreeMatrix.size());
			indegreeMatrix.forEach((k,v)->{if(k==null||v==null||v.contains(null)){System.out.println("k:"+k+"v:"+v);System.exit(-1);}else System.out.println(k+":"+Arrays.toString(v.toArray()));});
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
    		System.out.println("___>"+indegreeMatrix.size()+"<____");
    		//outDegreeCount=outDegreeCount.entrySet().stream().filter(entry->(entry.getKey()<=partitionSize)).collect(Collectors.toConcurrentMap(t -> t.getKey(), t -> t.getValue()));
    		//problem
    		outDegreeCount=outDegreeCount.entrySet().stream().filter(entry-> adjMatrix.get(entry.getKey()).stream().anyMatch(out->indegreeMatrix.containsKey(out))).collect(Collectors.toConcurrentMap(t -> t.getKey(), t -> t.getValue()));
    	}
    	else{
    		//RECEIVE indegree matrix and outdegree count from root node
    		ConcurrentMap<Integer, List<Integer>> indegreeMatrixBuffer[] = new ConcurrentHashMap[1];
    		ConcurrentMap<Integer,Integer> outDegreeCountBuffer[] = new ConcurrentHashMap[1];
    		MPI.COMM_WORLD.Recv(indegreeMatrixBuffer, 0, 1, MPI.OBJECT, 0, 1);
    		MPI.COMM_WORLD.Recv(outDegreeCountBuffer, 0, 1, MPI.OBJECT, 0, 2);
    		indegreeMatrix=indegreeMatrixBuffer[0];
    		System.out.println("indegree matrix rcvd");
    		indegreeMatrix.entrySet().stream().forEach(entry->System.out.println("k:"+entry.getKey()+",v:"+Arrays.toString(entry.getValue().toArray())));
    		outDegreeCount=outDegreeCountBuffer[0];
    		System.out.println("outdegree count rcvd");
    		outDegreeCount.entrySet().stream().forEach(entry->System.out.println("k:"+entry.getKey()+",v:"+entry.getValue()));
    	}
		distributeSize();
	}
	/**
	 * Distributes the size of the whole dataset
	 */
	private void distributeSize(){
		final int rankValuesBuffer[] = new int[1];
		rankValuesBuffer[0] = size;
		MPI.COMM_WORLD.Bcast(rankValuesBuffer, 0, 1, MPI.INT, 0);
		if(processID != ROOT_NODE_ID){
			size=rankValuesBuffer[0];
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
			System.out.println("Global Page Rank rcvd");
			rankValues.entrySet().stream().forEach(entry->System.out.println("k:"+entry.getKey()+",v:"+entry.getValue().getVal()));
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
    	if (processID == ROOT_NODE_ID) {
    	adjMatrix.keySet()
    				.parallelStream()
    				.forEach(key->rankValues.put(key,new WrappedDouble(1.0/size)));
    	}
    	final double DF_FACTOR=(1-df)/size;
    	IntStream
    	.rangeClosed(1, iterations)
    	.forEach(i->{
    		//using inbound approach for calculating ranks
    		distributeGlobalPageRank();
    		System.out.println(rankValues.size());
    		System.out.println("****DEBUG*****");
    		System.out.println(indegreeMatrix.size());
    		System.out.println("****DEBUG*****");
    		
    		ConcurrentMap<Integer, WrappedDouble> rankValuesIntermmediate=
    				indegreeMatrix.entrySet().parallelStream()
    				.collect(Collectors.
    						toConcurrentMap(
    								t -> t.getKey(), 
    								t->{
    									return new WrappedDouble
    											(t.getValue()
    													//making it parallel makes errors
    													.stream()
    													.mapToDouble(p->p)
    													//.peek(e->System.out.println("sum:"+e) )
    													.reduce(0,(a,b)->{
    														
//    														System.out.println(a+":"+(int)a);
//    														System.out.println(b+":"+(int)b);
//    														System.out.println("a rank val:"+rankValues.get((int)a).getVal());
//    														System.out.println("a odc:"+outDegreeCount.get((int)a));
//    														System.out.println("b rank val:"+rankValues.get((int)b).getVal());
//    														System.out.println("b odc:"+outDegreeCount.get((int)b));

    														return (a
    																+
    																(rankValues.get((int) b).getVal()/outDegreeCount.get((int) b)));}
    													)
    													).multiply(df).add(DF_FACTOR);}));
    		if (processID == ROOT_NODE_ID) {
    			IntStream
    	    	.range(1,nodes).forEach(j->{
    	    		//combine all the ranks at root node
    	    		final ConcurrentMap<Integer, WrappedDouble> rankValuesBuffer[] = new ConcurrentHashMap[1];
    	    		MPI.COMM_WORLD.Recv(rankValuesBuffer, 0, 1, MPI.OBJECT, j, 3);
    	    		System.out.println("Rcvd Intermmediate Rank from node:"+"j");
    	    		rankValuesBuffer[0].entrySet().stream().forEach(entry->System.out.println("k:"+entry.getKey()+",v:"+entry.getValue().getVal()));

    	    		rankValuesIntermmediate.putAll(rankValuesBuffer[0]);
    	    	});
    			rankValues=rankValuesIntermmediate;
    		}
    		else{
    			final ConcurrentMap<Integer, WrappedDouble> rankValuesBuffer[] = new ConcurrentHashMap[1];
    			rankValuesBuffer[0] = rankValuesIntermmediate;
    			System.out.println("Sending Intermmediate Rank");
    			rankValuesIntermmediate.entrySet().stream().forEach(entry->System.out.println("k:"+entry.getKey()+",v:"+entry.getValue().getVal()));

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
	        
	        
	        PageRank mpiPR = new PageRankMPIDebug(MPI.COMM_WORLD.Rank(),MPI.COMM_WORLD.Size());
	        mpiPR.parseArgs(args);
	        mpiPR.loadInput();
	        mpiPR.calculatePageRank();
	        mpiPR.printValues();
	        mpiPR.printExecutionTime(start);
	        MPI.Finalize();
	        
	}
	public void printExecutionTime(long start){
		if (processID == ROOT_NODE_ID) {
			System.out.println("Execution Completed in:"+(System.currentTimeMillis()-start)+"ms");
		}
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
