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

public class SequentialPageRank implements PageRank{
	/**
	 * To maintain consistency with parallel version. No utility as such.
	 * @author Siddharth Jain
	 *
	 */
	private static class WrappedDouble implements Comparable<WrappedDouble>{
		private double val;
		WrappedDouble(double val){
			this.val=val;
		}
		public void  add(double val1){
			val+=val1;
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
    private ConcurrentMap<Integer, WrappedDouble> rankValues = new ConcurrentHashMap<Integer, WrappedDouble>();

    /**
     * Parse the command line arguments and update the instance variables. Command line arguments are of the form
     * <input_file_name> <output_file_name> <num_iters> <damp_factor>
     *
     * @param args arguments
     */
    public void parseArgs(String[] args) {
    	if(args.length<4){
    		System.err.println("Please invocate the program with following arguments [input file name] [output file name] [iteration count] [damping factor]");
    		System.exit(-1);
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
		//split each line 
		.map(line->line.trim().split(" "))
		//populate adjacency matrix with given splitted lines
		.forEach((splittedLine)->{
			adjMatrix.put(
					Integer.valueOf(Integer.parseInt(splittedLine[0])), 
					Arrays
					.stream(splittedLine)
					.skip(1)
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
    	adjMatrix.keySet().stream().forEach(key->rankValues.put(key,new WrappedDouble(1.0/size)));
    	
    	IntStream
    	.rangeClosed(1, iterations)
    	.forEach(i->{
    		//using outbound approach for calculating ranks
    		//Map for intermmediate rank values
    		ConcurrentMap<Integer, WrappedDouble> rankValuesIntermmediate=adjMatrix.keySet().stream().collect(Collectors.toConcurrentMap(Function.identity(), t->new WrappedDouble(0.0))); 		
    		adjMatrix.entrySet().stream()
    		.forEach(entry->{
    			//handle dangling node
    			if(entry.getValue().size()==0){
    				rankValuesIntermmediate.keySet().stream()
    				.forEach(node->rankValuesIntermmediate.get(node).add(rankValues.get(entry.getKey()).getVal()/size));	
    			}
    			else{
    				entry.getValue().stream().forEach(outBoundNode->
    					rankValuesIntermmediate
    					.get(outBoundNode)
    					.add(rankValues.get(entry.getKey()).getVal()/entry.getValue().size()));
    			}
    			});
    		final double DF_FACTOR=(1-df)/size;
    		//factor in the damping factor
    		rankValuesIntermmediate.entrySet()
    		.stream()
    		.forEach(entry->entry.getValue().multiply(df).add(DF_FACTOR));
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
                .stream()
                //sort by page rank
                .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
                //limit to top 10
                //.limit(10)
                //create output
                .forEachOrdered(entry->output.append("Page: "+entry.getKey()+" : Rank: "+entry.getValue().getVal()+"\n"));
    	String outputResult=output.toString();
    	System.out.println("No of iterations:"+iterations+"\n"+outputResult.substring(0, getNthOccurenceOf(outputResult, "\n", 10)));
    	//writing output to file
    	Files.write(Paths.get(outputFile),outputResult.getBytes());
    }

    public static void main(String[] args) throws IOException {
        PageRank sequentialPR = new SequentialPageRank();
        long start=System.currentTimeMillis();
        sequentialPR.parseArgs(args);
        sequentialPR.loadInput();
        sequentialPR.calculatePageRank();
        sequentialPR.printValues();
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
