package com.bigdata.ml.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.ml.IKAnalyzer.IKAnalyzerProduct;

import java.io.*;

/**
 * @author zy
 */
public class HDFSFileOperations {

    private static final Logger logger = LoggerFactory.getLogger(HDFSFileOperations.class);

    public static final String FS_DEFAULT_NAME_KEY = CommonConfigurationKeys.FS_DEFAULT_NAME_KEY;

    public static String HDFS_STORE_DIR_PATH_PREFIX = "/bd/data";

    public static final String HDFS_STORE_DIR_PATH = "/daas/product/";

    public static final String CLIENT_USER = "smart_data";

    /**
     *
     * @param file
     * @param overwrite
     * @throws IOException
     */
    public static void addFile(final File file, boolean overwrite) throws IOException {
        addFile(file, overwrite, null);
    }
    /**
     * 
     * @param fileInputStream
     * @param overwrite
     * @throws IOException
     */
    public static void addFileStream(final InputStream inputStream, boolean overwrite,String lastpath) throws IOException {
    	addFileStream(inputStream, overwrite, null,lastpath);
    }
    public static BufferedReader readFileStream(String lastpath) throws IOException{
    		Configuration conf = new Configuration();
    		conf.addResource("core-site.xml");
            conf.addResource("hdfs-site.xml");

        String dest = HDFS_STORE_DIR_PATH_PREFIX + "/" + HDFS_STORE_DIR_PATH + "/"+lastpath;

        Path path = new Path(dest);

        // Initialize HDFS FileSystem.
        FileSystem fs = null;
        FSDataInputStream out = null;
        BufferedReader br=null;
       // String filterStrs=new String();
        try {
            try {
                fs = FileSystem.get(FileSystem.getDefaultUri(conf), conf, CLIENT_USER);
            } catch (InterruptedException e) {
            }
             FileStatus[] status=fs.listStatus(path);
             for(FileStatus file:status){
            	 String pathname=file.getPath().getName();
            	// System.out.println(pathname);
                 FSDataInputStream inputStream=fs.open(file.getPath());
                 br=new BufferedReader(new InputStreamReader(inputStream));
                 String line=null;
                 String returnStr="";
                 while(null!=(line=br.readLine())){   	                	 
                	  if(returnStr.getBytes().length>=524288){
                	    ByteArrayInputStream is = new ByteArrayInputStream(returnStr.getBytes()); 
                        addFileStream(is,Boolean.TRUE,"segmentData/"+pathname);  
                        returnStr="";
                	  }
                	  String res=IKAnalyzerProduct.startIKAnalyzer(line);
                	  if(res!=""){
                	    returnStr+=res;
                	  }
                 }
                 inputStream.close();
                 br.close();
            }
        } catch (Exception e) {
            throw e;
        } finally {
            if (br != null) {
                br.close();
            }
            if (out != null) {
                out.close();
            }
            if (fs != null) {
                fs.close();
            }
        }
        return br;
    }
    /**
     *
     * @param file
     * @param overwrite
     * @param conf
     * @throws IOException
     */
    public static void addFile(final File file, boolean overwrite, Configuration conf) throws IOException {

        if (file == null || !file.exists()) {
            throw new IllegalArgumentException("The file must be exists.");
        }

        if (!file.isFile()) {
            throw new IllegalArgumentException("Only support file, no directory");
        }

        if (conf == null) {
            conf = new Configuration();
            conf.addResource("hdfs-site.xml");
        }

        String filename = file.getName();

        String dest = HDFS_STORE_DIR_PATH_PREFIX + "/" + HDFS_STORE_DIR_PATH + "/" + filename;

        Path path = new Path(dest);

        // Initialize HDFS FileSystem.
        FileSystem fs = null;
        FSDataOutputStream out = null;
        InputStream in = null;
        try {
            try {
                fs = FileSystem.get(FileSystem.getDefaultUri(conf), conf, CLIENT_USER);
            } catch (InterruptedException e) {
            }

            if (!overwrite && fs.exists(path)) {
                logger.warn("File['dest'] already exists, and not support overWrite.");
                return;
            }
            out = fs.create(path, overwrite);

            in = new BufferedInputStream(new FileInputStream(file));

            byte[] buf = new byte[1024];

            int bytes = 0;
            while ((bytes = in.read(buf)) > 0) {
                out.write(buf, 0, bytes);
            }

            out.flush();

        } catch (Exception e) {
            throw e;
        } finally {
            if (in != null) {
                in.close();
            }
            if (out != null) {
                out.close();
            }
            if (fs != null) {
                fs.close();
            }
        }

    }
    /**
     * 
     * @param fileInputStream
     * @param overwrite
     * @param conf
     * @throws IOException
     */
    public static void addFileStream(final InputStream inputStream, boolean overwrite, Configuration conf,String lastpath) throws IOException {
        if (conf == null) {
            conf = new Configuration();
            conf.addResource("core-site.xml");
            conf.addResource("hdfs-site.xml");
        }

        String dest = HDFS_STORE_DIR_PATH_PREFIX + "/" + HDFS_STORE_DIR_PATH + "/"+lastpath;

        Path path = new Path(dest);

        // Initialize HDFS FileSystem.
        FileSystem fs = null;
        FSDataOutputStream out = null;
        InputStream in = null;
        try {
            try {
                fs = FileSystem.get(FileSystem.getDefaultUri(conf), conf, CLIENT_USER);
            } catch (InterruptedException e) {
            }

            if (!fs.exists(path)) {
            	 out = fs.create(path, overwrite);             
            }else{
            	out=fs.append(path);
            }
            in = new BufferedInputStream(inputStream);

            byte[] buf = new byte[1024];

            int bytes = 0;
            while ((bytes = in.read(buf)) > 0) {
                out.write(buf, 0, bytes);
            }

            out.flush();

        } catch (Exception e) {
            throw e;
        } finally {
            if (in != null) {
                in.close();
            }
            if (out != null) {
                out.close();
            }
            if (fs != null) {
                fs.close();
            }
        }

    }



}
