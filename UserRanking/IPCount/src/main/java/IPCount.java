import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IPCount {
	
	public static class IPCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text ip = new Text();
		private int lineNumber = 0;

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			lineNumber++;
			
			// Skip the first line (header)
			if(lineNumber == 1) {
				return;
			}
			
			String line = value.toString();
			String[] columns = line.split(",");
			if (columns.length > 0) {
				String ipAddress = columns[0].trim();
				ip.set(ipAddress);
				context.write(ip, one);
			}
		}
	}
	
	public static class IPCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
	    public void reduce(Text key, Iterable<IntWritable> values, Context context)
	            throws IOException, InterruptedException {
	        int sum = 0;
	        for (IntWritable value : values) {
	            sum += value.get();
	        }
	        context.write(key, new IntWritable(sum));
	    }
	}
	
	public static class CompositeKey implements WritableComparable<CompositeKey> {
		private int frequency;
	    private String ip;
	    
	    // Constructors
	    public CompositeKey() {
	    }
	    
	    public CompositeKey(int frequency, String ip) {
	        this.frequency = frequency;
	        this.ip = ip;
	    }

	    public String getIp() {
	        return ip;
	    }
	    
	    public int getFrequency() {
	        return frequency;
	    }
	    
	    public void set(int frequency, String ip) {
	    	this.ip = ip;
	    	this.frequency = frequency;
	    }
	    
	    public void write(DataOutput out) throws IOException {
	        out.writeInt(frequency);
	        out.writeUTF(ip);
	    }

	    public void readFields(DataInput in) throws IOException {
	        frequency = in.readInt();
	        ip = in.readUTF();
	    }

	    public int compareTo(CompositeKey other) {
	        // First, compare by frequency in descending order
	        int frequencyComparison = Integer.compare(other.frequency, this.frequency);
	        
	        // If frequencies are equal, compare by IP in ascending order
	        if (frequencyComparison == 0) {
	            return this.ip.compareTo(other.ip);
	        }
	        
	        return frequencyComparison;
	    }
	}

	public class CompositeKeyComparator extends WritableComparator {
		protected CompositeKeyComparator() {
	        super(CompositeKey.class, true);
	    }

	    @SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable w1, WritableComparable w2) {
	        CompositeKey key1 = (CompositeKey) w1;
	        CompositeKey key2 = (CompositeKey) w2;

	        // Custom sorting logic based on frequency (in descending order) and IP (in ascending order)
	        int frequencyComparison = Integer.compare(key2.getFrequency(), key1.getFrequency());

	        if (frequencyComparison == 0) {
	            return key1.getIp().compareTo(key2.getIp());
	        }

	        return frequencyComparison;
	    }
	}
	
	public static class SortMapper extends Mapper<LongWritable, Text, CompositeKey, Text> {
		private final CompositeKey compositeKey = new CompositeKey();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Parse the input and extract IP and frequency (your key-value format from the first cycle)
            String line = value.toString();
            String[] parts = line.split("\\s+");
            String ip = parts[0];
            int frequency = Integer.parseInt(parts[1]);

            // Set the frequency and IP as the composite key
            compositeKey.set(frequency, ip);

            // Pass IP as the value
            context.write(compositeKey, new Text(ip));
		}
	}

    public static class SortReducer extends Reducer<CompositeKey, Text, Text, IntWritable> {
    	public void reduce(CompositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Output each IP with its frequency in descending order
            for (Text ip : values) {
                context.write(ip, new IntWritable(key.getFrequency()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // First MapReduce cycle to get IP frequencies
        Job job1 = Job.getInstance(conf, "IPCount1");
        
        // Number of reduce tasks in the first cycle
        //job1.setNumReduceTasks(1);
        //job1.setNumReduceTasks(2);
        job1.setNumReduceTasks(4);
        
        job1.setJarByClass(IPCount.class);
        job1.setMapperClass(IPCountMapper.class);
        job1.setReducerClass(IPCountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));
        
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Second MapReduce cycle for custom sorting
        Job job2 = Job.getInstance(conf, "IPCount2");
        
        // Number of reduce tasks in the second cycle
        //job2.setNumReduceTasks(1);
        //job2.setNumReduceTasks(2);
        job2.setNumReduceTasks(4);
        
        job2.setJarByClass(IPCount.class);
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);
        job2.setOutputKeyClass(CompositeKey.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1] + "/temp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

