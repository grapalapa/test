package sk.erni.db.dbUpdater;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

public class DBUpdater {
	private String path;
	private String propertiesPath = "./src/main/java/update.properties";
//	private static String path = "C:/sputnik/Sputnik/dbupdate/db_2012-11-16_1_WEB-627_PROD.sql";
	private Connection connection;
	private Statement statement;
	private Logger log = Logger.getLogger(DBUpdater.class);
	
	
	public DBUpdater() {
		initialize();
	}
	
	public DBUpdater(String path) {
		this.path = path;
		initialize();
	}
	
	private void initialize() {
		Properties properties = new Properties();
		
		try {
			properties.load(new FileReader(propertiesPath));
			
			String url = properties.getProperty("mysql_url");
			String user = properties.getProperty("user");
			String pwd = properties.getProperty("pwd");
			
			this.connection = DriverManager.getConnection(url, user, pwd);
			this.connection.setAutoCommit(false);
			this.statement = connection.createStatement();
			
			if (path == null) {
				this.path = properties.getProperty("default_path");
			}

		} catch (SQLException e) {
			log.error("Error setting connection!", e);
			try {
				connection.close();
			} catch (SQLException e1) {
				log.error("Error closing connection!", e);
			}
		} catch (Exception e) {
			log.error(e);
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		DBUpdater dbUpdater = new DBUpdater();
		dbUpdater.runStatements();
	}

	private List<String> getStatements(File scriptFile) throws IOException {
		List<String> statementList = new ArrayList<String>(); 
		BufferedReader br = new BufferedReader(new FileReader(scriptFile));
		String line;
		StringBuilder builder = new StringBuilder();
		boolean comment = false;
		
		while ((line = br.readLine()) != null) {
			if (line.trim().startsWith("--") || "".equals(line.trim())) { 
				continue;
			} else if (line.trim().startsWith("/*")) {
				comment = true;
				continue;
			} else if (line.trim().endsWith("*/")) {
				comment = false;
				continue;
			} else if (comment) {
				continue;
			}
			
			builder.append(" ").append(line);
			if (line.endsWith(";")) {
				statementList.add(builder.toString());
				builder = new StringBuilder();
			}
		}
		
		return statementList;
	}
	
	public void runStatements() {
		File root = new File(path);
		
		for (File scriptFile : root.listFiles()) {
			try {
				for (String statementString : getStatements(scriptFile)) {
					statement.execute(statementString);
				}
				
				connection.commit();
				logUpdate(scriptFile.getName());
					
			} catch (Exception e) { 
				try {
					connection.rollback();
					log.error("Error executing sql script: '" + scriptFile.getName() + "' !", e);
					throw new RuntimeException();
				} catch (SQLException e1) {
					log.error("Error rollbacking transaction!", e1);
					throw new RuntimeException();
				}
			}
		}
		
		class 
	}

	private void logUpdate(String fileName) throws SQLException {
		Statement statement = connection.createStatement();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String timestamp = dateFormat.format(new Date());
		ScriptFile scriptFile = new ScriptFile(fileName);

		String insertSQL = "insert into db_webportal.dbupdate (name, scriptDate, idx, timestamp) values ('" + 
							fileName + "', '" + 
							scriptFile.getScriptDate() + "', '" + 
							scriptFile.getIndex() + "', '" + 
							timestamp + "');";
		
		statement.execute(insertSQL);
		connection.commit();
	}

	public void cleanDB(String table) throws SQLException {
		statement.execute("DROP TABLE " + table + ";");
		System.out.println();
	}
}
