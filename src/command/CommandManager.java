package command;

import java.util.HashMap;
import java.util.Map;

public class CommandManager {

	
	private Map<String, Command> map;
	

	
	
	public CommandManager(){
		map = new HashMap<String, Command>();
		init();
	}
	
	
	
	private void init(){
		MappingCommandClass(new InitCommand());
		MappingCommandClass(new StatusCommand());
		MappingCommandClass(new GetCommand());
		MappingCommandClass(new RangeCommand());
		MappingCommandClass(new AdjustCommand());
		MappingCommandClass(new SourceCommand());
		MappingCommandClass(new PutCommand());
		MappingCommandClass(new QuitCommand());
		MappingCommandClass(new HaltCommand());
	}
	
	private void MappingCommandClass(Command command){
		String[] names = command.getNames();
		for(int i=0;i<names.length;i++){
			map.put(names[i], command);
		}
	}
	
	
	public Map<String, Command> getCommandTable(){
		return map;
	}
	
	public Command createInstanceOf(String commandName) throws InstantiationException, IllegalAccessException{
		return map.get(commandName);
	}
	
	
	
	
	
	//not require.
	/*public List<Command> createInstacesOfAll() throws InstantiationException, IllegalAccessException{
		List<Command> list = new ArrayList<Command>();
		
		for(int i=0; i<command.length;i++){
			list.add((Command)map.get(command[i]));
		}
		return list;
	}*/
	


}
