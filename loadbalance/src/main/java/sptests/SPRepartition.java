package sptests;

import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InternalException;

/**
 * Created by yuyinghao on 9/4/18.
 */
public class SPRepartition {

    public static void main(String[] args){
        FileSystem fs = FileSystem.Factory.get();
        int isParallel = Integer.parseInt(args[0]);
        try{
            fs.spRepartition(isParallel==1);
        }catch(Exception e){
            e.printStackTrace();
        }
    }


}
