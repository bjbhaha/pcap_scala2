

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
 
/**
 * @author chaird
 * @create 2020-10-11 15:53
 */
public class ShellUtils {
  /**
   * @param pathOrCommand 脚本路径或者命令
   * @return
   */
  public static List<String> exceShell(String pathOrCommand) {
    List<String> result = new ArrayList<>();
 
    try {
      // 执行脚本
      Process ps = Runtime.getRuntime().exec(pathOrCommand);
      int exitValue = ps.waitFor();
      if (0 != exitValue) {
        System.out.println("call shell failed. error code is :" + exitValue);
      }
 
      // 只能接收脚本echo打印的数据，并且是echo打印的最后一次数据
      BufferedInputStream in = new BufferedInputStream(ps.getInputStream());
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      String line;
      while ((line = br.readLine()) != null) {
        System.out.println("脚本返回的数据如下： " + line);
        result.add(line);
      }
      in.close();
      br.close();
 
    } catch (Exception e) {
      e.printStackTrace();
    }
 
    return result;
  }
}