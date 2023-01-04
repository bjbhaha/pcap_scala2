import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * CSV操作(读取和写入)
 * @author lq
 * @version 2018-04-23
 */
public class CSVUtils {

    /**
     * 写入csv
     * @param file csv文件(路径+文件名)，csv文件不存在会自动创建
     * @param dataList 数据
     * @return
     */
    public static boolean exportCsv(File file, List<String> dataList){
        boolean isSucess=false;

        FileOutputStream out=null;
        OutputStreamWriter osw=null;
        BufferedWriter bw=null;
        try {
            out = new FileOutputStream(file);
            osw = new OutputStreamWriter(out);
            bw =new BufferedWriter(osw);
            if(dataList!=null && !dataList.isEmpty()){
                for(String data : dataList){
                    bw.append(data).append("\r");
                }
            }
            isSucess=true;
        } catch (Exception e) {
            isSucess=false;
        }finally{
            if(bw!=null){
                try {
                    bw.close();
                    bw=null;
                } catch (IOException e) {
                    //e.printStackTrace();
                    //注释打印调试信息(代码审计修改)
                }
            }
            if(osw!=null){
                try {
                    osw.close();
                    osw=null;
                } catch (IOException e) {
                    //e.printStackTrace();
                    //注释打印调试信息(代码审计修改)
                }
            }
            if(out!=null){
                try {
                    out.close();
                    out=null;
                } catch (IOException e) {
                    //e.printStackTrace();
                    //注释打印调试信息(代码审计修改)
                }
            }
        }

        return isSucess;
    }

    /**
     * 读csv
     * @param file csv文件(路径+文件)
     * @return
     */
    public static List<String> importCsv(File file){
        List<String> dataList=new ArrayList<String>();

        BufferedReader br=null;
        try {
            br = new BufferedReader(new FileReader(file));
            String line = "";
            while ((line = br.readLine()) != null) {
                dataList.add(line);
            }
        }catch (Exception e) {
        }finally{
            if(br!=null){
                try {
                    br.close();
                    br=null;
                } catch (IOException e) {
                    //e.printStackTrace();
                    //注释打印调试信息(代码审计修改)
                }
            }
        }

        return dataList;
    }


    /**
     * 测试
     * @param args
     */
    public static void main(String[] args){
        //exportCsv();
        //importCsv();
    }




    /**
     * CSV读取测试
     * @throws Exception
     */
    public static void importCsv()  {
        List<String> dataList=CSVUtils.importCsv(new File("D:/test/ljq.csv"));
        if(dataList!=null && !dataList.isEmpty()){
            for(int i=0; i<dataList.size();i++ ){
                if(i!=0){//不读取第一行
                    String s=dataList.get(i);
                    System.out.println("s  "+s);
                    String[] as = s.split(",");
                    System.out.println(as[0]);
                    System.out.println(as[1]);
                    System.out.println(as[2]);
                }
            }
        }
    }

    /**
     * CSV写入测试
     * @throws Exception
     */
    public static void exportCsv() {
        List<String> dataList=new ArrayList<String>();
        dataList.add("0-10,10-20,20-30");
        dataList.add("11:21:22,1927,84");
        dataList.add("2,li,male");
        dataList.add("3,hong,female");
        String fp = "/home/ysh/IdeaProjects/pcap_scala3/repo/test.csv";
        boolean isSuccess=CSVUtils.exportCsv(new File(fp), dataList);
        System.out.println(isSuccess);
    }


}

