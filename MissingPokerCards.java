import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MissingPokerCards {

  public static class PorkerMapper 
       extends Mapper<Object, Text, IntWritable, Text> {
    
	private final static IntWritable one = new IntWritable(1);	
	
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException 
    {
    	context.write(one,value);      	
    }
  }
  
  public static class PorkerReducer 
       extends Reducer<IntWritable,Text,Text,Text> {
    
	private Text wordRank = new Text();
	private Text wordSuit = new Text();
	
    public void reduce(IntWritable key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
    	
    	int i,j;
    	int[][] cards=new int[15][5];
    	for(i=0;i<15;i++) {for (j=0;j<5;j++) {cards[i][j]=-1;}};
    	
    	int cardRank=0;
    	int cardSuit=0;
    	
    	String[] rank=new String[]{"0","1","2","3","4","5","6","7","8","9","10","J","Q","K","A"};
    	String[] suit=new String[]{"0","club","diamond","heart","spade"};
    	
    	for (Text val : values) 
    	{
    		    		
    		String[] tuple = val.toString().split(" ");
    		if (tuple[0].compareTo("2")==0 
    	    		|| tuple[0].compareTo("3")==0 
    	    		|| tuple[0].compareTo("4")==0
    	    		|| tuple[0].compareTo("5")==0
    	    		|| tuple[0].compareTo("6")==0
    	    		|| tuple[0].compareTo("7")==0
    	    		|| tuple[0].compareTo("8")==0
    	    		|| tuple[0].compareTo("9")==0
    	    		|| tuple[0].compareTo("10")==0)
    	    {
    			cardRank=Integer.parseInt(tuple[0]);            
    	    }
    	    if (tuple[0].compareTo("J")==0)
    	    {
    	    	cardRank=11;        		
    	    }
    	    if (tuple[0].compareTo("Q")==0)
    	    {
    	    	cardRank=12;
    	    }
    	    if (tuple[0].compareTo("K")==0)
    	    {
    	    	cardRank=13;        		
    	    }
    	    if (tuple[0].compareTo("A")==0)
    	    {
    	    	cardRank=14;        		
    	    }
    	    
    	    if (tuple[1].compareTo("club")==0)
            {
    	    	cardSuit=1;        				
            }
        	if (tuple[1].compareTo("diamond")==0) 
            {
        		cardSuit=2;        		
            }
        	if (tuple[1].compareTo("heart")==0)
            {
        		cardSuit=3;        		
            }
        	if (tuple[1].compareTo("spade")==0)
            {
        		cardSuit=4;        		
            }
        	
        	cards[cardRank][cardSuit]=1;        	   	
    	}
    	
    	for(i=2;i<=14;i++)
    	{
    		for(j=1;j<=4;j++)
    		{
    			if (cards[i][j]==-1)
    			{
    				wordRank.set(rank[i]);
    				wordSuit.set(suit[j]);
    				context.write(wordRank, wordSuit);
    			}
    		}
    	}
    	
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: hadoop jar MissingPokerCards.jar <in> <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "MissingPokerCards");
    job.setJarByClass(MissingPokerCards.class);
    //job.setNumReduceTasks(1);
    
    job.setMapperClass(PorkerMapper.class);
    job.setReducerClass(PorkerReducer.class);
    
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
