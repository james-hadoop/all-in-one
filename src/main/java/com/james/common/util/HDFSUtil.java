package com.james.common.util;

//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.io.OutputStream;
//import java.net.MalformedURLException;
//import java.net.URL;
//import java.text.MessageFormat;
//import java.text.ParseException;
//import java.text.SimpleDateFormat;
//import java.util.ArrayList;
//import java.util.Calendar;
//import java.util.Date;
//import java.util.HashSet;
//import java.util.List;
//
//import javax.annotation.PostConstruct;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.LocatedFileStatus;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.PathFilter;
//import org.apache.hadoop.fs.RemoteIterator;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.stereotype.Service;
//import org.springframework.web.multipart.MultipartFile;

public class HDFSUtil {
    // private static final Logger LOGGER =
    // LoggerFactory.getLogger(HDFSUtil.class);
    //
    // public static String TASKSCHEDULER_JOB_HDFS_BASE_DIR =
    // "hdfs://localhost:8020/apps/taskscheduler_jobs";
    //
    // private FileSystem fileSystem = null;
    // private HashSet<String> hadoopConfigurationResourceUrl = new HashSet<>();
    //
    // @PostConstruct
    // public void init() throws Exception {
    // try {
    // for (String configResource :
    // WebConfig.GetInstance().getHdfsConfigPath().split(",")) {
    // hadoopConfigurationResourceUrl.add(configResource);
    // }
    //
    // fileSystem = FileSystem.get(getConfiguration());
    // } catch (Exception e) {
    // LOGGER.error("init hdfs file system failed.", e);
    // throw e;
    // }
    // }
    //
    // public HashSet<String> getHadoopConfigurationResourceUrl() {
    // return hadoopConfigurationResourceUrl;
    // }
    //
    // public void setHadoopConfigurationResourceUrl(HashSet<String>
    // hadoopConfigurationResourceUrl) {
    // this.hadoopConfigurationResourceUrl = hadoopConfigurationResourceUrl;
    // }
    //
    // public FileSystem getFileSystem() {
    // return fileSystem;
    // }
    //
    // public void setFileSystem(FileSystem fileSystem) {
    // this.fileSystem = fileSystem;
    // }
    //
    // public void close() throws Exception {
    // if (fileSystem != null) {
    // fileSystem.close();
    // }
    // }
    //
    // public List<Path> listFile(Path root) throws FileNotFoundException,
    // IOException {
    // return listFile(root, null);
    // }
    //
    // public List<Path> listFile(Path root, PathFilter pathFilter) throws
    // FileNotFoundException, IOException {
    // List<Path> fileList = new ArrayList<Path>();
    //
    // if (!fileSystem.exists(root)) {
    // return fileList;
    // }
    //
    // if (fileSystem.isFile(root)) {
    // fileList.add(root);
    // return fileList;
    // } else {
    // RemoteIterator<LocatedFileStatus> subFiles = fileSystem.listFiles(root,
    // true);
    // while (subFiles.hasNext()) {
    // fileList.add(subFiles.next().getPath());
    // }
    // return fileList;
    // }
    // }
    //
    // public List<Path> listPaths(Path basePath) throws FileNotFoundException,
    // IOException {
    // List<Path> fileList = new ArrayList<Path>();
    //
    // if (!fileSystem.exists(basePath)) {
    // return fileList;
    // }
    //
    // if (fileSystem.isFile(basePath)) {
    // return fileList;
    // } else {
    // FileStatus[] subFiles = fileSystem.listStatus(basePath);
    // for (FileStatus fileStatus : subFiles) {
    // if (fileStatus.isDirectory()) {
    // fileList.add(fileStatus.getPath());
    // }
    // }
    // return fileList;
    // }
    // }
    //
    // public List<FileStatus> listPathsAndFiles(Path basePath) throws
    // FileNotFoundException, IOException {
    // List<FileStatus> fileList = new ArrayList<FileStatus>();
    //
    // if (!fileSystem.exists(basePath)) {
    // return fileList;
    // }
    //
    // if (fileSystem.isFile(basePath)) {
    // return fileList;
    // } else {
    // FileStatus[] subFiles = fileSystem.listStatus(basePath);
    // for (FileStatus fileStatus : subFiles) {
    // fileList.add(fileStatus);
    // }
    // return fileList;
    // }
    // }
    //
    // public List<Path> listPathsBetweenDays(Path basePath, Calendar startTime,
    // Calendar endTime, String dateFormat)
    // throws Exception {
    // List<Path> retList = new ArrayList<>();
    // while (startTime.before(endTime)) {
    // String datePart = DateUtil.DateToString(startTime.getTime(), dateFormat);
    // Path datePath = new Path(basePath.toUri().getPath() + "/" + datePart);
    // retList.addAll(listPaths(datePath));
    //
    // startTime.add(Calendar.DAY_OF_MONTH, 1);
    // }
    //
    // return retList;
    // }
    //
    // public long getPathSize(Path root) throws IOException {
    //
    // long result = 0;
    // if (!fileSystem.exists(root)) {
    // return result;
    // }
    //
    // if (fileSystem.isFile(root)) {
    // FileStatus rootFileStatus = fileSystem.getFileStatus(root);
    // return rootFileStatus.getLen();
    // }
    //
    // if (fileSystem.isDirectory(root)) {
    // RemoteIterator<LocatedFileStatus> subFiles = fileSystem.listFiles(root,
    // true);
    // while (subFiles.hasNext()) {
    // result += subFiles.next().getLen();
    // }
    // }
    //
    // return result;
    // }
    //
    // public void createNewHDFSFile(String toCreateFilePath, String content)
    // throws IOException {
    // FSDataOutputStream os = fileSystem.create(new Path(toCreateFilePath));
    // os.write(content.getBytes("UTF-8"));
    // os.close();
    // }
    //
    // public void createNewHDFSFile(String toCreateFilePath) throws IOException
    // {
    // fileSystem.create(new Path(toCreateFilePath));
    // }
    //
    // public List<String> hdfsFileRead(String filePath) throws IOException {
    // List<String> results = new ArrayList<String>();
    // BufferedReader reader = null;
    // try {
    // Path pathq = new Path(filePath);
    // if (fileSystem.isFile(pathq)) {
    // FSDataInputStream inputStream = fileSystem.open(pathq);
    // reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
    // String line = "";
    // while ((line = reader.readLine()) != null) {
    // line = reader.readLine();
    // if (line != null) {
    // results.add(line);
    // }
    // }
    // }
    //
    // } catch (Exception e) {
    // e.printStackTrace();
    // throw e;
    // } finally {
    // if (reader != null) {
    // reader.close();
    // }
    // }
    //
    // return results;
    // }
    //
    // public boolean hdfsFileReName(String oldHdfsPath, String newHdfsPath)
    // throws IOException {
    // boolean isRename = false;
    // Path frpaht = new Path(oldHdfsPath);
    // Path topath = new Path(newHdfsPath);
    // isRename = fileSystem.rename(frpaht, topath);
    // return isRename;
    // }
    //
    // // 递归删除路径下所有文件
    // public void hdfsFileDelete(String hdfsFilePath) throws IOException {
    // Path frpaht = new Path(hdfsFilePath);
    // if (!fileSystem.exists(frpaht)) {
    // return;
    // }
    // // 递归删除
    // fileSystem.delete(frpaht, true);
    // }
    //
    // // HDFS文件目录创建
    // public void hdfsDirectoryCreate(String hdfsDirPath) throws IOException {
    // Path frpaht = new Path(hdfsDirPath);
    // fileSystem.mkdirs(frpaht);
    // }
    //
    // public void uploadFile(String localFilePath, String remoteFilePath)
    // throws IOException {
    // File localFile = new File(localFilePath);
    // if (!localFile.exists()) {
    // throw new FileNotFoundException("File " + localFile.getAbsolutePath());
    // } else if (!localFile.isFile()) {
    // throw new IOException("File " + localFile.getAbsolutePath() +
    // " is not a file");
    // }
    //
    // Path remoteFile = new Path(remoteFilePath);
    //
    // if (fileSystem.exists(remoteFile)) {
    // fileSystem.delete(remoteFile, false);
    // }
    //
    // fileSystem.copyFromLocalFile(new Path(localFile.getPath()), remoteFile);
    // }
    //
    // public void downLoadFile(Path remoteFile, String localFilePath) throws
    // IOException {
    // File localFile = new File(localFilePath);
    // if (localFile.exists()) {
    // localFile.delete();
    // }
    //
    // Path localPath = new Path(localFilePath);
    // fileSystem.copyToLocalFile(false, remoteFile, localPath, true);
    // }
    //
    // public void uploadDir(String localDir, String remoteDir) throws
    // IOException {
    // File localDirFile = new File(localDir);
    // if (!localDirFile.exists()) {
    // throw new FileNotFoundException("Dir " + localDirFile.getAbsolutePath());
    // } else if (!localDirFile.isDirectory()) {
    // throw new IOException("Path " + localDirFile.getAbsolutePath() +
    // " is not a dir");
    // }
    // for (File localFilePath : localDirFile.listFiles()) {
    // if (localFilePath.isFile()) {
    // String remoteFilePath = remoteDir + File.separator +
    // localFilePath.getName();
    // uploadFile(localFilePath.getAbsolutePath(), remoteFilePath);
    // }
    // }
    // }
    //
    // public void downLoadDir(String remoteDir, String localDir) throws
    // IOException {
    // Path remotePath = new Path(remoteDir);
    // if (!fileSystem.exists(remotePath)) {
    // throw new
    // FileNotFoundException(MessageFormat.format("hdfs dir {0} is not found.",
    // remoteDir));
    // }
    //
    // File localPath = new File(localDir);
    // if (!localPath.exists()) {
    // throw new
    // FileNotFoundException(MessageFormat.format("local dir {0} is not found.",
    // localDir));
    // }
    //
    // for (Path remoteFilePath : listFile(remotePath)) {
    // String localFilePath = localDir + File.separator +
    // remoteFilePath.getName();
    // downLoadFile(remoteFilePath, localFilePath);
    // }
    // }
    //
    // public boolean validate(String path, Calendar startTime, Calendar
    // endTime) {
    // boolean ret = false;
    //
    // String[] pathArray = path.split("/");
    // String fileName = pathArray[pathArray.length - 1];
    //
    // SimpleDateFormat timeFormat = new SimpleDateFormat("yyyyMMdd");
    //
    // String[] fileNameArray = fileName.split(".");
    // String fileTime = fileNameArray[fileNameArray.length - 4];
    //
    // try {
    //
    // Date fileDate = (Date) timeFormat.parse(fileTime);
    //
    // Calendar fileCal = Calendar.getInstance();
    //
    // fileCal.setTime(fileDate);
    //
    // if (fileCal.compareTo(startTime) > 0 && fileCal.compareTo(endTime) < 0) {
    // ret = true;
    // }
    //
    // } catch (ParseException e) {
    // e.printStackTrace();
    // }
    //
    // return ret;
    // }
    //
    // public Configuration getConfiguration(String cluster) throws
    // MalformedURLException {
    // return getConfiguration();
    // }
    //
    // public Configuration getConfiguration() throws MalformedURLException {
    // Configuration conf = new Configuration();
    // for (String resource : hadoopConfigurationResourceUrl) {
    // conf.addResource(new URL(resource));
    // }
    //
    // return conf;
    // }
    //
    // public boolean writeFile(MultipartFile file, String hdfsFileName) throws
    // IOException {
    // if (null == file || file.isEmpty() || null == hdfsFileName || 0 ==
    // hdfsFileName.length()) {
    // return false;
    // }
    //
    // OutputStream os = null;
    // try {
    // os = fileSystem.create(new Path(hdfsFileName), true);
    // os.write(file.getBytes());
    // } catch (Exception e) {
    //
    // } finally {
    // if (null != os) {
    // os.close();
    // }
    // }
    //
    // return true;
    // }
    //
    // public void createDirIfNotExists(String dirName) throws IOException {
    // Path dirPath = new Path(dirName);
    // if (!fileSystem.exists(dirPath)) {
    // fileSystem.mkdirs(dirPath);
    // }
    // }
    //
    // public boolean isDirExists(String dirName) throws IOException {
    // Path dirPath = new Path(dirName);
    // if (!fileSystem.exists(dirPath)) {
    // return false;
    // } else {
    // return true;
    // }
    // }
    //
    // public static boolean uploadFiles(String fileName) throws
    // FileNotFoundException {
    // Path path = new Path(TASKSCHEDULER_JOB_HDFS_BASE_DIR);
    //
    // File localFile = new File(fileName);
    // if (!localFile.exists()) {
    // throw new FileNotFoundException("File " + localFile.getAbsolutePath());
    // }
    //
    // return false;
    // }
}
