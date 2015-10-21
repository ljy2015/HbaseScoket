package com.yiban.datacenter.finalversion;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class DealUserThread implements Runnable {

	private String testconnect = "username=chenpiao,password=123;username=liujiyu,password=123"; // 这个可以用来验证用户名和密码

	private static Configuration conf = HBaseConfiguration.create();

	private static Connection connection = null;
	
	private String logFile=null;
	// 配置信息
	static {
		try {
			conf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.27.233");
			conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2181);
			connection = ConnectionFactory.createConnection(conf);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}

	private Socket s;

	public DealUserThread(Socket s) {
		this.s = s;
	}

	private String userTableName = "nihao";
	private String columnFamilyName = null;
	private String rowKey = null;

	private BufferedReader serverread = null;
	private BufferedWriter serverwrite = null;

	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			// 将通道内的字节流转换成字符流，并用bufferedreader进行封装，InputStreamReader是将字节流转换成字符流
			serverread = new BufferedReader(new InputStreamReader(
					s.getInputStream()));

			// 询问客户端连接是否准备好，接受客户端的连接请求
			String line = serverread.readLine(); // 阻塞
			//System.out.println(line);// 输出客户端的连接请求
			
			//为日志文件命名,并创建文件
			SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd hh:mm:ss"); 
			logFile="/var/log/datacenter/"+"user.log";
			//System.out.println(logFile);
			File destFile = new File(logFile);
			if (!destFile.exists()) {
				destFile.createNewFile();
			}
			writeByFileWrite(logFile, line+"\n"+sdf.format(System.currentTimeMillis()));
			// 将通道内的字符写入到对应的文件中，利用bufferedwrite进行封装,FileWriter是将字符流写入到文件中
			serverwrite = new BufferedWriter(new OutputStreamWriter(
					s.getOutputStream()));
			String[] strArray = testconnect.split("\\;");
			boolean flag = false;
			for (String str : strArray) {
				if (str.equals(line)) {
					/*
					 * serverwrite.write("连接成功，你可以发送数据了，发送数据前，请先发送你要用的数据库表名！");
					 * serverwrite.newLine(); serverwrite.flush();
					 */
					printInfomationForClient("connection successful ,now you can send data,befor send data ,you must send tablename!");
					flag = true;
					break;
				}
			}

			if (!flag) {
				/*
				 * serverwrite.write("密码或者用户名错误，连接失败！"); serverwrite.newLine();
				 * serverwrite.flush();
				 */
				printInfomationForClient("username or password is error! connection failed!");
				s.close();
			}
			
			// 准备接收表名
			userTableName = serverread.readLine();
			//System.out.println("tablename:" + userTableName);// 输出客户端的连接请求的表名
			
			writeByFileWrite(logFile, "tablename="+userTableName);//将内容写到日志文件中
			
			// 告诉客户端，我接受成功
			if (TableIsExist(userTableName)) {
				printInfomationForClient("received tablename successful!");
			} else {
				printInfomationForClient("tablename Is not exist ");
				s.close();
			}


			line = "[";
			StringBuffer temp = new StringBuffer(line);
			while ((line = serverread.readLine()) != null) {
				temp.append(line);
			}
			temp.append("]");
			//System.out.println(temp.toString());

			// 对接收到的数据进行异常处理，如上传的数据格式不正确等等。
			try {
				// 对json文件进行解析
				JSONArray jsonArray = JSONArray.fromObject(temp.toString());
				// JSONObject jsonobject=JSONObject.fromObject(temp.toString());

				// 解析之后进行输出
				//PrintJsonArray(jsonArray);

				//获取所有的表名
				//getAllTables(conf);

				// 将接收到的数据写入hbase中的表中
				insertData(jsonArray, userTableName);
				
				//System.out.println("存入数据成功！");

			} catch (Exception e) {
				printErrorForClient(e);
			}
			// 给出一个反馈，提示数据上传成功
			// 封装通道内的输出流，方便对他进行写字符数据
			// BufferedWriter bwserver = new BufferedWriter(new
			// OutputStreamWriter(s.getOutputStream()));

			/*
			 * serverwrite.write("文件上传成功！"); // bwserver.newLine();
			 * serverwrite.flush(); serverwrite.close();
			 */
			printInfomationForClient("upload data successful!");

			// 释放资源
			s.close();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			try {
				printErrorForClient(e);
				writeByFileWrite(logFile,e.getMessage()+e.toString());
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}

	private void printInfomationForClient(String s) throws IOException {
		try {
			serverwrite.write(s);
			writeByFileWrite(logFile, s);//将内容写到日志文件中
			serverwrite.newLine();
			serverwrite.flush();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			writeByFileWrite(logFile, e.getMessage()+e.toString());//将内容写到日志文件中
		}
	}

	private void printErrorForClient(Exception e) throws IOException {
		try {
			serverwrite.write("found a error:" + e.getMessage() + e.toString());
			writeByFileWrite(logFile,"found a error:" + e.getMessage() + e.toString());
			serverwrite.newLine();
			serverwrite.flush();
			s.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			writeByFileWrite(logFile,e1.getMessage()+e1.toString());
		}

	}

	Map<String, String> colvalue = new TreeMap<String, String>();

	private void insertData(JSONArray jsonArray, String userTableName)
			throws IOException {

		// connect the table
		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(userTableName));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			printErrorForClient(e1);
		}

		colvalue.clear();

		for (int i = 0; i < jsonArray.size(); i++) {
			JSONObject obj = jsonArray.getJSONObject(i);
			Set<String> keysets = obj.keySet();
			for (String key : keysets) {
				switch (key) {
				case "category":
					columnFamilyName = obj.getString(key);
					break;
				case "version":
					colvalue.put("version", obj.getString("version"));
					break;
				case "DocumentType":
					colvalue.put("DocumentType", obj.getString("DocumentType"));
					break;
				case "articles":
					JSONArray articlesjars = obj.getJSONArray("articles");
					dealjsonArray(table,articlesjars);
					break;
				default:
					printErrorForClient(new Exception("send datatype is error!"));
				}
			}

		}
	}

	private void insertColDataToHbase(Table table) throws IOException {
		// 判断是否包含对应的列族，若不包含则添加
		HTableDescriptor desc = new HTableDescriptor(table.getName());
		Collection<HColumnDescriptor> familys=desc.getFamilies();
		if (  familys.contains(new HColumnDescriptor(columnFamilyName)) 
				&& columnFamilyName != null) {
			addColFamily(table, desc, columnFamilyName);
		}

		// insert data
		List<Put> putlist = new ArrayList<Put>();

		if (!colvalue.isEmpty() && rowKey != null && columnFamilyName != null) {
			Put put = new Put(Bytes.toBytes(rowKey));// 指定行,也就是键值
			// 下面就是循环存储列
			for (Entry<String, String> col : colvalue.entrySet()) {
				put.add(Bytes.toBytes(columnFamilyName),
						Bytes.toBytes(col.getKey()),
						Bytes.toBytes(col.getValue()));
				putlist.add(put);
			}

		} else {
			printErrorForClient(new Exception("send datatype is error!"));
		}

		try {
			table.put(putlist);
			table.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			printErrorForClient(e);
		}
	}

	private void dealjsonArray(Table table,JSONArray articlesjars) throws IOException {
		// TODO Auto-generated method stub
		if(articlesjars.isEmpty()){
			System.out.println("articlesjars is empty");
			printErrorForClient(new Exception("send datatype is error!"));
			return;
		}
		for(int i=0;i<articlesjars.size();i++){
			JSONObject obj=articlesjars.getJSONObject(i);
			Set<String>keysets=obj.keySet();
			for(String key:keysets){
				switch(key){
				case "content":
					colvalue.put("content", obj.getString("content"));
					break;
				case "picture_url":
					colvalue.put("picture_url", obj.getString("picture_url"));
					break;
				case "time":
					colvalue.put("time", obj.getString("time"));
					break;
				case "author":
					colvalue.put("author", obj.getString("author"));
					break;
				case "url":
					rowKey=obj.getString("url");
					break;
				case "title":
					colvalue.put("title", obj.getString("title"));
					break;
				default:
					printErrorForClient(new Exception("send datatype is error!"));
				}
			}
			try {
				insertColDataToHbase(table);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				printErrorForClient(e);
			}	
		}
		
	}

	private void addColFamily(Table table, HTableDescriptor desc,
			String colFamily) throws IOException {

		Admin ad = connection.getAdmin();

		HColumnDescriptor family = new HColumnDescriptor(
				Bytes.toBytes(colFamily));// 列簇
		desc.addFamily(family);
		ad.addColumn(table.getName(), family);
		ad.close();

	}

	private boolean TableIsExist(String userTableName2) throws IOException {
		boolean flag = false;
		try {
			// Connection connection = ConnectionFactory.createConnection(conf);
			Admin ad = connection.getAdmin();
			if (ad.tableExists(TableName.valueOf(userTableName2))) {
				flag = true;
				//System.out.println("表存在");
			} else {
				//System.out.println("表不存在");
				//printErrorForClient(new Exception("表不存在"));
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
			printErrorForClient(e);
		}

		return flag;
		// TODO Auto-generated method stub

	}

	private void PrintJsonArray(JSONArray jsonArray) {
		int size = jsonArray.size();
		System.out.println("Size: " + size);
		for (int i = 0; i < size; i++) {
			JSONObject jsonObject = jsonArray.getJSONObject(i);
			Set<String> keysets = jsonObject.keySet();
			for (String keyset : keysets) {
				System.out.println(keyset);
			}
		}
	}

	private void PrintJsonArray(JSONObject jsonobject, String... keys) {
		int size = jsonobject.size();
		System.out.println("Size: " + size);
		for (int i = 0; i < size; i++) {
			for (String key : keys) {
				System.out.println(key + ":" + jsonobject.get(key));
			}

			// System.out.println("[" + i + "]id=" + jsonObject.get("id"));
			// System.out.println("[" + i + "]name=" + jsonObject.get("name"));
			// System.out.println("[" + i + "]role=" + jsonObject.get("role"));
		}
	}

	//写日志文件
	public static void writeByFileWrite(String _sDestFile, String _sContent)
			throws IOException {
		FileWriter fw = null;
		try {
			fw = new FileWriter(_sDestFile,true);
			fw.write(_sContent);
			fw.write('\n');
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (fw != null) {
				fw.close();
				fw = null;
			}
		}
	}
	
	// create table
	private void createTable(Configuration conf) {
		// HBaseAdmin ha=new HBaseAdmin(conf);
		try {
			// Connection connection = ConnectionFactory.createConnection(conf);
			Table table = connection.getTable(TableName.valueOf(userTableName));
			Admin ad = connection.getAdmin();

			// TableName name= TableName.valueOf(Bytes.toBytes(tablename));//表名
			HTableDescriptor desc = new HTableDescriptor(table.getName());

			HColumnDescriptor family = new HColumnDescriptor(
					Bytes.toBytes(columnFamilyName));// 列簇
			desc.addFamily(family);

			ad.createTable(desc);
			ad.close();

		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}

	}

	// Hbase获取所有的表信息
	public static List getAllTables(Configuration conf)
			throws MasterNotRunningException, ZooKeeperConnectionException,
			IOException {

		HBaseAdmin ad = new HBaseAdmin(conf);
		List<String> tables = null;
		if (ad != null) {
			try {
				HTableDescriptor[] allTable = ad.listTables();
				if (allTable.length > 0)
					tables = new ArrayList<String>();
				for (HTableDescriptor hTableDescriptor : allTable) {
					tables.add(hTableDescriptor.getNameAsString());
					System.out.println(hTableDescriptor.getNameAsString());
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return tables;
	}

}
