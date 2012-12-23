package main;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import util.AlphanumericID;
import util.MyUtil;

public class MainTest extends MyUtil {
	
	private String distPachageName ="distributedIndex.";
	private String distMethod = distPachageName+"SkipGraph";
	private String portStr="18054";
	private String idStr = "util.AlphanumericID";//"user1000";
	private String seedStr ="end1";
	
	private int port = 18054;
	
	@Before
	public void beforeSetUp(){
		
	}
	
	
	
	@After
	public void afterSetUp(){
		
	}
	
	
	
	@Test
	public void testStart() throws InterruptedException{
		/*long ti = System.currentTimeMillis();
		Thread.sleep(1000);
		long time = System.currentTimeMillis();
		pri((time-ti)/1000+"");*/
		
		Main main = new Main();
		String[] args = {distMethod,idStr,portStr,seedStr};
		main.start(args);
		
		
	}
	
	
	
	
	
	
	
	
	
	
}
