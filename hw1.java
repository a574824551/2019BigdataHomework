import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import org.apache.log4j.*;

import java.util.ArrayList;
import java.util.Arrays;

public class Hw1Grp1 {
	private static String add_R;  //The address of file R
	private static String add_S;  //The address of file S
	private static Integer[] res_R;  //The names of the colomns in file R
	private static Integer[] res_S;  //The names of the colomns in file R
	private static ArrayList<String[]> table_R = new ArrayList<String[]>();  //The data read from file R
	private static ArrayList<String[]> table_S = new ArrayList<String[]>();  //The data read from file S
	private static ArrayList<String[]> join_res = new ArrayList<String[]>();  //The result after join
	private static ArrayList<String> key_value_R = new ArrayList<String>();  //The result after join
	private static ArrayList<String> key_value_S = new ArrayList<String>();  //The result after join
	
	public static void main(String[] args) throws IOException, URISyntaxException, MasterNotRunningException, ZooKeeperConnectionException {
		Input_args_init(args);
		Load_data_hdfs(add_R, res_R, table_R, key_value_R);
		Load_data_hdfs(add_S, res_S, table_S, key_value_S);
		Sort_merge_join(table_R, table_S, key_value_R, key_value_S);
		Hbase_hdfs(join_res);
	}
	
	public static void Input_args_init(String[] input_args) {
		ArrayList<Integer> res_R_list = new ArrayList<Integer>();
		ArrayList<Integer> res_S_list = new ArrayList<Integer>();
		//get file1 and file2 path and split it
		add_R = "hdfs://localhost:9000"+input_args[0].substring(2);
		add_S = "hdfs://localhost:9000"+input_args[1].substring(2);
		//get the key columns and transform string into integer
		String[] join_input = input_args[2].split("R|=|S");
		int R_join_key = Integer.parseInt(join_input[1]);
		int S_join_key = Integer.parseInt(join_input[3]);
		String[] res_list_tmp = input_args[3].substring(4).split(",");
		//get table R's and table S's result list
		for(int i=0; i<res_list_tmp.length;i++)
		{
			if(res_list_tmp[i].matches("R.*"))
				res_R_list.add(Integer.valueOf(res_list_tmp[i].substring(1)));
			else if (res_list_tmp[i].matches("S.*"))
				res_S_list.add(Integer.valueOf(res_list_tmp[i].substring(1)));
		}
		res_R = new Integer[res_R_list.size()+1];
		res_S = new Integer[res_S_list.size()+1];
		res_R[0] = R_join_key;
		res_S[0] = S_join_key;
		for(int i = 0; i < res_R_list.size(); i++)
			res_R[i+1] = res_R_list.get(i);
		for(int i = 0; i < res_S_list.size(); i++)
			res_S[i+1] = res_S_list.get(i);	
	}
	
	public static void Load_data_hdfs(String file, Integer[] res, ArrayList<String[]> table, ArrayList<String> key_value) throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(file), conf);
		Path path = new Path(file);
		FSDataInputStream in_stream = fs.open(path);
		BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
		String s;
		int line_count=0;
		while ((s=in.readLine())!=null) {//load a line from hdfs
			String[] table_temp = s.split("\\|");
			String[] table_data = new String[res.length - 1];
			for(int j = 0; j < table_data.length; j++)
				table_data[j] = table_temp[res[j+1]];
			table.add(table_data);
			key_value.add(table_temp[res[0]] + '+' + String.valueOf(line_count));
			line_count++;
		}
		in.close();
		fs.close();
	}
	
	public static void Sort_merge_join(ArrayList<String[]> tableR, ArrayList<String[]> tableS, ArrayList<String> key_valueR, ArrayList<String> key_valueS) {
		int count = 0;//number of same key value 
		int r = 0;//firs one in table R
		int s = 0;//firs one in table S
		
		String[] join_key_R = new String[key_valueR.size()];
		String[] join_key_S = new String[key_valueS.size()];
		
		String[] key_value_R = new String[key_valueR.size()];
		String[] key_value_S = new String[key_valueS.size()];
		
		int[] line_R = new int[key_valueR.size()]; 
		int[] line_S = new int[key_valueS.size()]; 
	
		String[] join_temp_split_R;
		String[] join_temp_split_S;
		String[] join_temp_split;
		
		for(int i = 0; i < tableR.size(); i++)
			join_key_R[i] = key_valueR.get(i);
		for(int i = 0; i < tableS.size(); i++)
			join_key_S[i] = key_valueS.get(i);
		
		Arrays.sort(join_key_R);//sort on R's key value
		Arrays.sort(join_key_S);//sort on S's key value
		
		while(r < key_valueR.size() && s < key_valueS.size()) {
			//split the sigh to get the real key value and the line number
			join_temp_split_R = join_key_R[r].split("\\+");
			join_temp_split_S = join_key_S[s].split("\\+"); 
			key_value_R[r]=join_temp_split_R[0];
			key_value_S[s]=join_temp_split_S[0];
			line_R[r]=Integer.valueOf(join_temp_split_R[1]);
			line_S[s]=Integer.valueOf(join_temp_split_S[1]);

			//scan from the min value of table R and table S to find same key value of them 
			if(key_value_R[r].compareTo(key_value_S[s]) < 0){
				r++;
			} else if(key_value_R[r].compareTo(key_value_S[s]) > 0) {
				s++;
			} else{
				count = 0;//count return to 0
				for(int s_find_index = s; s_find_index < key_value_S.length; s_find_index++){//find from the s_find_indexrent value of s
					join_temp_split = join_key_S[s_find_index].split("\\+");
					if(key_value_S[s].equals(join_temp_split[0])){//find out the same key value
						String[] join_tmp = new String[res_S.length + res_R.length];
						for(int j = 0; j < res_R.length; j++) {//load R into join_res
							if(j==0){
								join_tmp[j] = key_value_R[r];
							}
							else{
								join_tmp[j] = tableR.get(line_R[r])[j - 1];
							}
						}
						for(int j = 1; j < res_S.length ; j++) {//load S into join_res
							join_tmp[j + res_R.length -1] = tableS.get(Integer.valueOf(join_temp_split[1]))[j - 1];
						}
						join_tmp[res_S.length + res_R.length-1] = String.valueOf(count);
						count++;
						join_res.add(join_tmp);//add to result list 
					}
				}
				r++;//the next one of table R
			}
		}
	}
	
	public static void Hbase_hdfs(ArrayList<String[]> join_res) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		Logger.getRootLogger().setLevel(Level.WARN);
		String tableName= "Result";
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
		HColumnDescriptor res = new HColumnDescriptor("res");
		htd.addFamily(res);
		Configuration configuration = HBaseConfiguration.create();
		HBaseAdmin hAdmin = new HBaseAdmin(configuration);
		hAdmin.createTable(htd); 
		hAdmin.close();
		HTable table = new HTable(configuration,tableName);
		String row_key = new String();
		String column = new String();
		int len = res_R.length + res_S.length - 1;
		System.out.println(len);
		for(int i = 0; i < join_res.size(); i++){
			row_key = join_res.get(i)[0];
			for(int j = 1; j < res_R.length; j++){//Formatted output
				if(!join_res.get(i)[len].equals("0"))//multiple R data of the same key value
					column = "R" + String.valueOf(res_R[j]) + "." + join_res.get(i)[len];
				else{//sigle R data of the same key value
					column = "R" + String.valueOf(res_R[j]);
				}
				Put put = new Put(row_key.getBytes());
				put.add("res".getBytes(),column.getBytes(),(join_res.get(i)[j]).getBytes());
				table.put(put);				
			}
			for(int j = res_R.length; j < len; j++){
				if(!join_res.get(i)[len].equals("0"))//multiple S data of the same key value
					column = "S" + String.valueOf(res_S[j+1-res_R.length]) + "." + join_res.get(i)[len];
				else{//sigle S data of the same key value
					column = "S" + String.valueOf(res_S[j+1-res_R.length]);
				}
				Put put = new Put(row_key.getBytes());
				put.add("res".getBytes(),column.getBytes(),(join_res.get(i)[j]).getBytes());
				table.put(put);
			}
		}
		table.close();
	}
}
