import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.DoubleWritable.Comparator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRank extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int res = ToolRunner.run(new PageRank(), args);
		System.exit(res);

	}

	public int run(String[] args) throws Exception {
		int result = 0;
		int i = 0;
		
		// First job to parse the title and links in that page
		Job job = Job.getInstance(getConf(), " wordcount ");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path("output"+i));
		job.setMapperClass(PageLinkMap.class);
		job.setReducerClass(PageLinkReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Second job to calculate the page rank of each page
		//This jobs runs multiple times and takes input as a output of the first job
		if (job.waitForCompletion(true)) {
			//number of iterations of second job
			for (; i <10 ; i++) {
				Job job2 = Job.getInstance(getConf(), " wordcount ");
				job2.setJarByClass(this.getClass());
				// String in = "output" + i;
				// String out = "output" + (i + 1);
				FileInputFormat.addInputPaths(job2, args[1] + i);
				FileOutputFormat.setOutputPath(job2, new Path(args[1]+ (i + 1)));
				job2.setMapperClass(PageRankMap.class);
				job2.setReducerClass(PageRankReduce.class);
				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(Text.class);
				result = job2.waitForCompletion(true) ? 0 : 1;
			}
			
		}
		//Third job sorts the page rank of each page and remove the unnecessary information
		if(result == 0){
		Job job3 = Job.getInstance(getConf(), " wordcount ");
		job3.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(job3, args[1] +i);
		FileOutputFormat.setOutputPath(job3, new Path(args[1]+"_final"));
		job3.setMapperClass(sortMap.class);
		job3.setReducerClass(sortReduce.class);
		//creates only one reducer for this job3
		job3.setNumReduceTasks(1);
		job3.setOutputKeyClass(DoubleWritable.class);
		job3.setOutputValueClass(Text.class);
		// sort the output in descending order
		job3.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		return job3.waitForCompletion(true) ? 0 : 1;
		}
		
		return result;
		// return job.waitForCompletion(true) ? 0 : 1;
		//return result;
	}

	// Mapper of the first job takes each page as a input and gives it's title and linked pages
	// output of the map will be - 
	//		Page A 	Page B
	//		Page A 	Page c
	public static class PageLinkMap extends Mapper<LongWritable, Text, Text, Text> {

		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			String line = lineText.toString();
			try
			{
			if (line != null && !line.trim().equals("")) {
				// parse the title from the line
				String title = StringUtils.substringBetween(line, "<title>", "</title>");
				
				// parse the text between <text and </text>
				String text = StringUtils.substringBetween(line, "<text", "</text>");
				
				//gets the linked pages for this page 
				String page[] = StringUtils.substringsBetween(text, "[[", "]]");
				for (int i = 0; i < page.length; i++) {
					context.write(new Text(title.trim()), new Text(page[i].trim()));
				}

			}
			}
			catch(Exception e)
			{
				return;
			}
		}

	}
	//Reducer of the first job 
	// it will gives title initial page rank and the links
	//output will look like Page A 	1.0 	PAge B^@@@^Page C
	public static class PageLinkReduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text title, Iterable<Text> link, Context context) throws IOException, InterruptedException {

			String rank = "1.0\t";
			boolean first = true;

			//iterating over the links for the same title
			for (Text t : link) {
				if (!first) {
					rank = rank + "^@@@^";
				}
				rank = rank + t.toString();
				first = false;
			}

			context.write(title, new Text(rank));
		}

	}

	// Mapper of the second job it is counting the total outgoing links for each outgoing page
	//output of the map will look like PAge B Page A 1.0(rank) 7(total_links)
		public static class PageRankMap extends Mapper<LongWritable, Text, Text, Text> {

		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			String line = lineText.toString();
			int page_index = line.indexOf("\t");
			int rank_index = line.indexOf("\t", page_index + 1);

			if(page_index == -1)
				return;
			
			if(rank_index == -1)
				return;
			
			
			String page = line.substring(0, page_index);		
			String rank = line.substring(0, rank_index + 1);
			
			context.write(new Text(page), new Text("!"));

			//if page does not contain any link skip that page 
			if (rank_index == -1)
				return;

			String links = line.substring(rank_index + 1, line.length());
			links = links.trim();

			// getting total of outgoing links
			String link[] = links.split("^@@@^");
			int total_links = link.length;

			// getting outgoing pages 
			for (int i = 0; i < link.length; i++) {
				String value = rank + total_links;
				context.write(new Text(link[i]), new Text(value));
			}
			
			context.write(new Text(page), new Text("$" + links));
		}

	}

	//Reduce for the second job it will calculate the new page rank and write the output in same format as the output of previous job
	//Output of the reduce look like - Page A 1.25 Page B Page C
	public static class PageRankReduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text title, Iterable<Text> link, Context context) throws IOException, InterruptedException {

			double otherPageRanks = 0.0;
			double damping = 0.85;
			boolean isExistingPage = false;
			String links = "";

			for (Text li : link) {
				String pageRank = li.toString();

				if (pageRank.equals("!")) {
					isExistingPage = true;
					continue;
				}

				if (pageRank.startsWith("$")) {
					links = "\t" + pageRank.substring(1);
					continue;
				}

				String split[] = pageRank.split("\\t");

				if(split[1].equals(""))
					return;
				if(split[2].equals(""))
					return;
				double rank = Double.valueOf(split[1]);
				int outLink = Integer.valueOf(split[2]);

				// calculate the page rank
				otherPageRanks += (rank / outLink);

			}
			if (!isExistingPage)
				return;

			Double updatedRank =  (damping * otherPageRanks + (1 - damping));

			context.write(title, new Text(updatedRank + links));
		}
	}

	//Mapper of the third job it sorts the key in descending order 
	//output of the map 1.25 Page A
	public static class sortMap extends Mapper<LongWritable, Text, DoubleWritable, Text> {

		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		//private static int count = 0;
		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			
			try{
			String line = lineText.toString();
			String[] pageRank = new String[2];
			
			int page_index = line.indexOf("\t");
			int rank_index = line.lastIndexOf("\t");			
			
			pageRank[0]= line.substring(0, page_index);
			pageRank[1]=line.substring(page_index+1, rank_index);
			
			if(pageRank[0].equals(""))
				return;
			if(pageRank[1].equals(""))
				return;
			
			// removing the extra things from the values and make the rank key and pages value
			Text value = new Text(pageRank[0]);
			DoubleWritable key = new DoubleWritable(Double.parseDouble(pageRank[1].trim()));
				
			//LongWritable key = new LongWritable(Double.parseDouble(pageRank[1]));
			
			context.write(key, value);
			}catch(Exception e){
				return;
			}
							
		}

	}
	
	public static class sortReduce extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {

		public void reduce(DoubleWritable title, Iterable<Text> link, Context context) throws IOException, InterruptedException {

			for(Text page : link){
				context.write(title, new Text(page));
			}
			
		}

	}
	

}
