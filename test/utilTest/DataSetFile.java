package utilTest;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class DataSetFile{

	BufferedReader bufReader ;
	private String filePath;
	private File file;
	String line;
	
	public DataSetFile(String filePath) {
		this.file = new File(filePath);
	}
	
	public void start() throws FileNotFoundException{
		bufReader = new BufferedReader(new FileReader(file));
	}
	
	public String readLine() throws IOException{
		return bufReader.readLine();
	}
	
	public void end() throws IOException{
		bufReader.close();
	}
	

}
