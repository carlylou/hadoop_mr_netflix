import org.apache.hadoop.mapred.JobConf;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by liumengyao on 16/11/22.
 */
public class Recommend {

    public static final String HDFS = "hdfs://localhost:9000";
    public static final Pattern DELIMITER = Pattern.compile("[\t,]");

    public static void main(String[] args) throws Exception {
        //配置输入输出路径
        Map<String, String> path = new HashMap<String, String>();
        path.put("data", "/Users/liumengyao/Documents/DataScience/Netflix/small.csv");
        path.put("Step1Input", HDFS + "/user/hdfs/recommend");
        path.put("Step1Output", path.get("Step1Input") + "/step1");
        path.put("Step2Input", path.get("Step1Output"));
        path.put("Step2Output", path.get("Step1Input") + "/step2");
        path.put("Step3Input1", path.get("Step1Output"));
        path.put("Step3Output1", path.get("Step1Input") + "/step3_1");
        path.put("Step3Input2", path.get("Step2Output"));
        path.put("Step3Output2", path.get("Step1Input") + "/step3_2");
        path.put("Step4Input1", path.get("Step3Output1"));
        path.put("Step4Input2", path.get("Step3Output2"));
        path.put("Step4Output", path.get("Step1Input") + "/step4");

        //启动各个子环节mapreduce
        Step1.run(path);
        Step2.run(path);
        Step3.run1(path);
        Step3.run2(path);
        Step4.run(path);
        System.exit(0);
    }

    //统一设置资源文件
    public static JobConf config() {
        JobConf conf = new JobConf(Recommend.class);
        conf.setJobName("Recommend");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }
}
