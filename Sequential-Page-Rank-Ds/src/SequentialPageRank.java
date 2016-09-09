import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SequentialPageRank {
	
	public static class AtomicDouble implements Comparable<AtomicDouble>{
		private double val;
		AtomicDouble(double val){
			this.val=val;
		}
		public synchronized void  add(double val1){
			val+=val1;
		}
		public synchronized AtomicDouble  multiply(double val1){
			val*=val1;
			return this;
		}
		public double getVal(){
			return val;
		}
		@Override
		public int compareTo(AtomicDouble other){
			return Double.compare(val, other.getVal());
		}
	}
	
    // adjacency matrix read from file
    private Map<Integer, List<Integer>> adjMatrix = new ConcurrentHashMap<>();
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
    private ConcurrentMap<Integer, AtomicDouble> rankValues = new ConcurrentHashMap<Integer, AtomicDouble>();

    /**
     * Parse the command line arguments and update the instance variables. Command line arguments are of the form
     * <input_file_name> <output_file_name> <num_iters> <damp_factor>
     *
     * @param args arguments
     */
    public void parseArgs(String[] args) {
    	if(args.length<3){
    		System.out.println("Please invocate the program with following arguments [input file name] [output file name] [iteration count] [damping factor]");
    	}
    	else{
    		inputFile=args[0];
    		outputFile=args[1];
    		iterations=Integer.parseInt(args[2]);
    		df=Double.parseDouble(args[3]);
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
    	Path inputFilePath=Paths.get(inputFile);
    	//read all lines
		Files.lines(inputFilePath)
		.parallel()
		//split each line 
		.map(line->line.trim().split(" "))
		//populate adjacency matrix with given splitted lines
		.forEach((splittedLine)->{
			adjMatrix.put(
					Integer.valueOf(Integer.parseInt(splittedLine[0])), 
					Arrays
					.stream(splittedLine)
					.skip(1)
					.parallel()
					.map(el->Integer.parseInt(el))
					.collect(Collectors.toList()));
		});
    	//initialize size
    	size=adjMatrix.size();
    }

    /**
     * Do fixed number of iterations and calculate the page rank values. You may keep the
     * intermediate page rank values in a hash table.
     */
    public void calculatePageRank() {
    	
    	//iteration 0: initialize the page rank of all the nodes to 1/n
    	adjMatrix.keySet().parallelStream().forEach(key->rankValues.put(key,new AtomicDouble(1.0/size)));
    	
    	IntStream
    	.rangeClosed(1, iterations)
    	.forEach(i->{
    		//using outbound approach for calculating ranks
    		ConcurrentMap<Integer, AtomicDouble> rankValuesIntermmediate=adjMatrix.keySet().parallelStream().collect(Collectors.toConcurrentMap(Function.identity(), t->new AtomicDouble(0.0))); 		
    		adjMatrix.entrySet().parallelStream()
    		.forEach(entry->{
    			//handle dangling node
    			if(entry.getValue().size()==0){
    				rankValuesIntermmediate.keySet().stream().parallel()
    				.forEach(node->rankValuesIntermmediate.get(node).add(rankValues.get(entry.getKey()).getVal()/size));	
    			}
    			else{
    				entry.getValue().forEach(outBoundNode->
    					rankValuesIntermmediate
    					.get(outBoundNode)
    					.add(rankValues.get(entry.getKey()).getVal()/entry.getValue().size()));
    			}
    			});
    		//factor in the damping factor
    		rankValuesIntermmediate.entrySet().parallelStream().forEach(entry->entry.getValue().multiply(df).add((1-df)/size));
    		rankValues=rankValuesIntermmediate;
    	});
    	    	
    	
    }
    			
    			
    	   

    /**
     * Print the pagerank values. Before printing you should sort them according to decreasing order.
     * Print all the values to the output file. Print only the first 10 values to console.
     *
     * @throws IOException if an error occurs
     */
    public void printValues() throws IOException {
    	//Map<Double,Integer> results=new TreeMap<>(Collections.reverseOrder());
    	StringBuilder output=new StringBuilder();
    	rankValues.entrySet()
                .parallelStream()
                //sort by page rank
                .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
                //limit to top 10
                .limit(10)
                //create output
                .forEachOrdered(entry->output.append("Page: "+entry.getKey()+" : Rank: "+entry.getValue().getVal()+"\n"));
    	String outputResult=output.toString();
    	System.out.println("No of iterations:"+iterations+"\n"+outputResult);
    	//writing output to file
    	Files.write(Paths.get(outputFile),outputResult.getBytes());
    }

    public static void main(String[] args) throws IOException {
        SequentialPageRank sequentialPR = new SequentialPageRank();
        sequentialPR.parseArgs(args);
        sequentialPR.loadInput();
        sequentialPR.calculatePageRank();
        sequentialPR.printValues();
    }
}
