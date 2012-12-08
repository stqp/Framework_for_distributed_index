package commandTest;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import command.Command;
import command.CommandManager;

public class CommandManagerTest {

	@Test
	public void test() {
		
		CommandManager cm = new CommandManager();
		Map<String, Command> li = cm.getCommandTable();
	}

}
