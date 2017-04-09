/**
 * Created by liumengyao on 16/11/22.
 * 得到用户对物品的评分矩阵
 * 1       102:3.0,103:2.5,101:5.0
 * 2       101:2.0,102:2.5,103:5.0,104:2.0
 * 3       107:5.0,101:2.0,104:4.0,105:4.5
 * 4       101:5.0,103:3.0,104:4.5,106:4.0
 * 5       101:4.0,102:3.0,103:2.0,104:4.0,105:3.5,106:4.0
 */
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Step1 {

    public static class Step1_ToItemPreMapper extends MapReduceBase implements Mapper<Object, Text, IntWritable, Text> {
        private final static IntWritable k = new IntWritable();
        private final static Text v = new Text();

        public void map(Object key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            String[] tokens = Recommend.DELIMITER.split(value.toString());//userId itemId pref
            int userID = Integer.parseInt(tokens[0]);
            String itemID = tokens[1];
            String pref = tokens[2];
            k.set(userID);
            v.set(itemID + ":" + pref);
            output.collect(k, v);//userId itemId:pref
            //System.out.println(k+" "+v);
        }
    }

    public static class Step1_ToUserVectorReducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
        private final static Text v = new Text();

//        public void reduce(IntWritable key, Iterator values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
//            StringBuilder sb = new StringBuilder();
//            while (values.hasNext()) {
//                sb.append("," + values.next());
//            }
//            v.set(sb.toString().replaceFirst(",", ""));
//            output.collect(key, v);
//        }

        //reduce collect all data with same key.
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            StringBuilder sb = new StringBuilder();
            while (values.hasNext()) {
                sb.append("," + values.next());
            }
            v.set(sb.toString().replaceFirst(",", ""));
            output.collect(key, v);
            System.out.println(key+" "+v);
        }
    }

    public static void run(Map<String, String> path) throws IOException {
        JobConf conf = Recommend.config();

        String input = path.get("Step1Input");
        String output = path.get("Step1Output");

        //将数据放在Step1Input下:
        HdfsDAO hdfs = new HdfsDAO(Recommend.HDFS, conf);
        hdfs.rmr(input);
        hdfs.mkdirs(input);
        hdfs.copyFile(path.get("data"), input);

        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Step1_ToItemPreMapper.class);
        conf.setCombinerClass(Step1_ToUserVectorReducer.class);
        conf.setReducerClass(Step1_ToUserVectorReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        RunningJob job = JobClient.runJob(conf);
        while (!job.isComplete()) {
            job.waitForCompletion();
        }
    }

}
