代码解释

```java
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
```

第一个mapper是预处理的mapper，将数据中的一行读入，分割成两个节点，第一个作为reducer中key，value是第二个节点+1。tol记录所有的边数，pagetol记录所有的node数。



```java
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
```

Reducer读取Mapper的输出，经过shuffle后，将key一样的键值对，聚合在一起，进行reduce。这里我们用一个hashset来处理有向边的可能重复的问题，同时用cnt来记录以当前节点为起点的边数，用$\frac{cnt}{tol}$ 作为初始的PageRank值。这里的key依然是起点，value是初始的PageRank值和所有的有向边的终点。



```java
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
```

id作为当前的网页，pr为从文本中读到的PageRank值，count为当前网页的出链网页个数。我们使用pr/count就能够得到当前网页对于出链网页的贡献值。这里的输出分为两种情况，分别以"&"和"#"做开头。以"#"开头的，将出链的网页作为key，value是当前网页对于出链网页的贡献值(计算总的PageRank值的时候，就为各个网页对其的贡献值之和，所以我们只需要将贡献值写到value中，之后shuffle的时候会将所有的对应节点的贡献值聚合在一起)。以"&"开头的，写入的是<当前网页，所有的出链的网页>，作为网页关系的存储，因为这个还会进行下一轮的迭代。



```java
public static class PRReduce extends Reducer<Text,Text,Text,Text>{     
        public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{            
            String link = "";
            float pr = 0;
            /*对values中的每一个val进行分析，通过其第一个字符是'@'还是'&'进行判断
            通过这个循环，可以 求出当前网页获得的贡献值之和，也即是新的PageRank值；同时求出当前
            网页的所有出链网页 */
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

```

选取0.2作为阻尼系数。计算出以"#"开头的value的和，即为pr值，代入公式进行计算，得到新的PageRank值，同时将网页关系存入，写到结果里，方便下一轮的计算。



```java
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
```

前半部分的job用来进行预处理，得到的结果写入文件中，下半部分进行正式的PageRank计算，迭代40轮，每一轮的结果写入文件中，最后得到结果。

