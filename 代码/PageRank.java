import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import java.util.StringTokenizer;
import org.apache.hadoop.util.GenericOptionsParser;




import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;

public class PageRank{
    /** tol是所有的边数*/
	public static int tol = 0;
	/** pagetol是所有的node数*/
	public static int pagetol = 0;
	public static class PreMapper extends Mapper<LongWritable,Text,Text,Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

			String line = value.toString();
        
            String[] words = line.split("\\s");

            tol += 1;
            context.write(new Text(words[0]),new Text(words[1]+" "+"1"));
		}
	}

	public static class PreReduce extends Reducer<Text,Text,Text,Text>{
		@Override
		protected void reduce(Text key,Iterable<Text> values, Context context) throws IOException, InterruptedException{
			pagetol++;
			Set<String> set = new HashSet<>();
			int cnt = 0;
			for(Text value:values){
				String [] words = value.toString().split("\\s");
				set.add(words[0]);
				cnt+=1;
			}
			String ans = new String(); 	
			for(String s : set){
				ans += " ";
				ans+=s;
			}
			float p = (float)cnt/tol;
			firstp.put(key,p);
			context.write(key,new Text(String.valueOf(p)+ans));
		}
	}

	public static class PRMapper extends Mapper<Object,Text,Text,Text>{        
        private String id;//id为解析的第一个词，代表当前网页
        private float pr;//pr为解析的第二个词，转换为float类型，代表PageRank值       
        private int count;//count为剩余词的个数，代表当前网页的出链网页个数
        private float average_pr;//求出当前网页对出链网页的贡献值       
        public void map(Object key,Text value,Context context) throws IOException,InterruptedException{
        	String []str = value.toString().split("\\s");
            id = str[0];
            pr = Float.parseFloat(str[1]);
            count = str.length-2;
            average_pr = pr/count;
            String linkids ="&";//下面是输出的两类，分别有'#'和'&'区分
            for(int i = 2;i < str.length;i++){
            	String linkid = str[i];
            	context.write(new Text(linkid),new Text("#"+average_pr));
				linkids +=" "+ linkid;
			}
            context.write(new Text(id), new Text(linkids));//输出的是<当前网页，所有出链网页>
        }       
    }

    public static class PRReduce extends Reducer<Text,Text,Text,Text>{     
        public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{            
            String link = "";
            float pr = 0;
            /*对values中的每一个val进行分析，通过其第一个字符是'#'还是'&'进行判断*/
            for(Text val:values){
                if(val.toString().substring(0,1).equals("#")){
                    pr += Float.parseFloat(val.toString().substring(1));
                }
                else if(val.toString().substring(0,1).equals("&")){
                    link += val.toString().substring(1);
                }
            }

            pr = 0.8f*pr + 0.2f*(1/pagetol);//加入跳转因子，进行平滑处理           
            String result = pr+link;
            context.write(key, new Text(result));           
        }
    }

	public static void main(String []args) throws Exception{
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Job job = Job.getInstance(conf,"PageRank");
		job.setJarByClass(PageRank.class);
		job.setMapperClass(PreMapper.class);
		job.setReducerClass(PreReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);

		String pathIn = args[1];
		String pathOut = "./output0";
		for(int i = 1;i <= 40;i++){
			job = Job.getInstance(conf,"PageRank");
			job.setJarByClass(PageRank.class);
			job.setMapperClass(PRMapper.class);
			job.setReducerClass(PRReduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(pathIn));
			FileOutputFormat.setOutputPath(job, new Path(pathOut));
			pathIn = pathOut;
			pathOut = "./output"+i;
			job.waitForCompletion(true);
		}
	}
}