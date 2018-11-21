package sptests;

import alluxio.client.file.FileSystem;

/**
 * Created by yyuau on 6/9/2018.
 */
public class GetK {
  public static void main(String[] args){
    FileSystem fs = FileSystem.Factory.get();
    try{
      fs.getK();
    }catch(Exception e){
      e.printStackTrace();
    }
  }
}
