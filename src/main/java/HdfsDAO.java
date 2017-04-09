/**
 * Created by liumengyao on 16/11/21.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.net.URI;

public class HdfsDAO {
    //HDFS访问地址
    private static final String HDFS = "hdfs://localhost:9000/";

    public HdfsDAO(Configuration conf) {
        this(HDFS, conf);
    }

    public HdfsDAO(String hdfs, Configuration conf) {
        this.hdfsPath = hdfs;
        this.conf = conf;
    }

    //hdfs路径
    private String hdfsPath;
    //Hadoop系统配置
    private Configuration conf;


    //加载Hadoop配置文件
    public static JobConf config(){
        JobConf conf = new JobConf(HdfsDAO.class);
        conf.setJobName("HdfsDAO");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }

    //API实现
//    public void cat(String remoteFile) throws IOException {...}
//    public void mkdirs(String folder) throws IOException {...}

    public void ls(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        FileStatus[] list = fs.listStatus(path);
        System.out.println("ls: " + folder);
        System.out.println("==========================================================");
        for (FileStatus f : list) {
            System.out.printf("name: %s, folder: %s, size: %d\n", f.getPath(), f.isDir(), f.getLen());
        }
        System.out.println("==========================================================");
        fs.close();
    }

    //启动函数

    /**
     * for ls testing
     *
     * @param args
     * @throws IOException
     */
//    public static void main(String[] args) throws IOException {
//        JobConf conf = config();
//        HdfsDAO hdfs = new HdfsDAO(conf);
//        hdfs.ls("/user/hdfs/recommend/step1");
//    }


    public void mkdirs(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        if (!fs.exists(path)) {
            fs.mkdirs(path);
            System.out.println("Create: " + folder);
        }
        else {
            System.out.println("The folder already exists: " + folder);
        }
        fs.close();
    }

    /**
     * for mkdirs testing
     *
     * @param args
     * @throws IOException
     */
//    public static void main(String[] args) throws IOException {
//        JobConf conf = config();
//        HdfsDAO hdfs = new HdfsDAO(conf);
//        hdfs.mkdirs("/user/hdfs/recommend");
//        //hdfs.mkdirs("/user/b/two");
//        hdfs.ls("/user/hdfs/recommend");
//    }


    public void rmr(String folder) throws IOException {
        Path path = new Path(folder);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.deleteOnExit(path);
        System.out.println("Delete: " + folder);
        fs.close();
    }

    /**
     * for rmr testing
     *Delete: /user/b/one
     *ls: /user/b
     *==========================================================
     *name: hdfs://localhost:9000/user/b/two, folder: true, size: 0
     *==========================================================
     * @param args
     * @throws IOException
     */
//    public static void main(String[] args) throws IOException {
//        JobConf conf = config();
//        HdfsDAO hdfs = new HdfsDAO(conf);
//        hdfs.rmr("/user/b/one");
//        hdfs.ls("/user/b");
//    }



    public void copyFile(String local, String remote) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.copyFromLocalFile(new Path(local), new Path(remote));
        System.out.println("copy from: " + local + " to " + remote);
        fs.close();
    }

    /**
     * for copyFile testing
     *
     * copy from: /Users/liumengyao/Documents/ML/tongdao_csv to /user/b/one
     ls: /user/b/one
     ==========================================================
     name: hdfs://localhost:9000/user/b/one/分享数.csv, folder: false, size: 348
     name: hdfs://localhost:9000/user/b/one/按天维度答完题各个分数段的人数.csv, folder: false, size: 1750
     name: hdfs://localhost:9000/user/b/one/每天注册准考证的人数.csv, folder: false, size: 369
     name: hdfs://localhost:9000/user/b/one/答题人信息.csv, folder: false, size: 380415
     name: hdfs://localhost:9000/user/b/one/.DS_Store, folder: false, size: 6148
     name: hdfs://localhost:9000/user/b/one/wxssj_uni建表语句.sql, folder: false, size: 1846
     ==========================================================
     * @param args
     * @throws IOException
     */
//    public static void main(String[] args) throws IOException {
//        JobConf conf = config();
//        HdfsDAO hdfs = new HdfsDAO(conf);
//        hdfs.copyFile("/Users/liumengyao/Documents/ML/tongdao_csv", "/user/b/one");
//        hdfs.ls("/user/b/one");
//    }

    public void cat(String remoteFile) throws IOException {
        Path path = new Path(remoteFile);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        FSDataInputStream fsdis = null;
        System.out.println("cat: " + remoteFile);
        try {
            fsdis =fs.open(path);
            IOUtils.copyBytes(fsdis, System.out, 4096, false);
        } finally {
            IOUtils.closeStream(fsdis);
            fs.close();
        }
    }

    /**
     * for cat testing
     * cat: /user/a/output1/part-r-00000
     Bye	1
     Goodbye	1
     Hadoop	2
     Hello	3
     World	3

     Process finished with exit code 0
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        JobConf conf = config();
        HdfsDAO hdfs = new HdfsDAO(conf);
        hdfs.cat("/user/hdfs/recommend/step4/part-00000");
    }

    public void download(String remote, String local) throws IOException {
        Path path = new Path(remote);
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        fs.copyToLocalFile(path, new Path(local));
        System.out.println("download: from" + remote + " to " + local);
        fs.close();
    }

    /**
     * for download from hdfs to local
     *download: from/user/a/output1/part-r-00000 to /Users/liumengyao/wordcount/dlfromhdfs
     *\/Users\/liumengyao\/wordcount\/dlfromhdfs\/part-r-00000
     * @param args
     * @throws IOException
     */
//    public static void main(String[] args) throws IOException {
//        JobConf conf = config();
//        HdfsDAO hdfs = new HdfsDAO(conf);
//        hdfs.download("/user/a/output1/part-r-00000", "/Users/liumengyao/wordcount/dlfromhdfs");
//
//        File f = new File("/Users/liumengyao/wordcount/dlfromhdfs/part-r-00000");
//        System.out.println(f.getAbsolutePath());
//    }

    public void createFile(String file, String content) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
        byte[] buff = content.getBytes();
        FSDataOutputStream os = null;
        try {
            os = fs.create(new Path(file));
            os.write(buff, 0, buff.length);
            System.out.println("Create: " + file);
        } finally {
            if (os != null)
                os.close();
        }
        fs.close();
    }

    /**
     * for createFile testing
     *
     *Create: /user/b/two/createFile
     cat: /user/b/two/createFile
     Hello world!!
     * @param args
     * @throws IOException
     */
//    public static void main(String[] args) throws IOException {
//        JobConf conf = config();
//        HdfsDAO hdfs = new HdfsDAO(conf);
//        hdfs.createFile("/user/b/two/createFile", "Hello world!!");
//        hdfs.cat("/user/b/two/createFile");
//    }


}
