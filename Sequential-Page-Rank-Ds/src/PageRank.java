import java.io.IOException;


public interface PageRank {
    public void parseArgs(String[] args);
    public void loadInput() throws IOException;
    public void calculatePageRank();
    public void printValues() throws IOException;
	public void printExecutionTime(long start);

}
