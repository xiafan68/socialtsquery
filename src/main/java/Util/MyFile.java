package Util;

import java.io.*;

/**
 * Created by dingcheng on 2015/1/7.
 */
public class MyFile {

    protected FileInputStream fis = null;
    protected InputStreamReader isr = null;
    protected BufferedReader br = null;

    protected String path = null;
    protected String encode = null;

    /**
     * 是否打开文件。
     */
    public boolean isOpen = false;


    public MyFile() {
    }

    public MyFile(String path, String encode) {
        this.path = path;
        this.encode = encode;
        try {
            open(path, encode);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 打开文件
     *
     * @param path
     * @throws java.io.UnsupportedEncodingException
     * @throws java.io.FileNotFoundException
     */
    public void open(String path, String encode) throws UnsupportedEncodingException, FileNotFoundException {
        File file = new File(path);
        if (!file.exists()) {
            return;
        }
        fis = new FileInputStream(path);
        isr = new InputStreamReader(fis, encode);
        br = new BufferedReader(isr);
        isOpen = true;
    }

    public void close() {
        try {
            fis.close();
            isr.close();
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        isOpen = false;
    }

    /**
     * 读取一行，如果末尾，返回null
     * @return
     * @throws java.io.IOException
     */
    public String readLine() throws IOException {
        return br.readLine();
    }

    public String read() {
        String line = null;
        String temp = null;
        StringBuffer buff = new StringBuffer();// 字符串缓存，存读入进来的字符串的
        try {
            while ((line = br.readLine()) != null) {
                // line = line.substring(line.indexOf("\t") + 1);
                // System.out.println(line);
                buff.append(line);
                buff.append("\r\n");
            }
            temp = buff.toString();
            buff.delete(0, buff.length());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return temp;
    }
}
