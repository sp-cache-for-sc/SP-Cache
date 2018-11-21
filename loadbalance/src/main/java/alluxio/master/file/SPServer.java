package alluxio.master.file;

import alluxio.AlluxioURI;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by yyuau on 25/12/2017.
 */
public class SPServer extends TimerTask{
    private ConcurrentHashMap<AlluxioURI, Long> mAccessHistory = new ConcurrentHashMap<AlluxioURI, Long>(); // thread safe?
    // private Long mTotalCount;
    // private Long mThreshold = 10L;
    private String mLog;

    public SPServer(){
        // mTotalCount = 0L; // log the access history every mThreshold accesses
        mLog = System.getProperty("user.dir") + "/logs/SPServer.txt"; // log the popularity
    }

    public void onAccess(AlluxioURI alluxioURI) {
        // mTotalCount += 1;

        mAccessHistory.putIfAbsent(alluxioURI, 0L); // atomic operation
        mAccessHistory.put(alluxioURI, mAccessHistory.get(alluxioURI)+1);

        //if (mAccessHistory.containsKey(alluxioURI)) {
          //  mAccessHistory.put(alluxioURI, mAccessHistory.get(alluxioURI)+1);
        //}
        //else
         //   mAccessHistory.put(alluxioURI, 1L);
        // if(mTotalCount % mThreshold == 0) {
           // doLog();
        // }
    }

    public void onCreate(AlluxioURI alluxioURI) {
        mAccessHistory.put(alluxioURI, 0L);
    }

    public void run() { // periodically log the access counts
        Iterator it = mAccessHistory.entrySet().iterator();
        FileWriter fw = null;
        try {
            fw = new FileWriter(mLog, true); //the true will append the new data
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                fw.write(pair.getKey() + " = " + pair.getValue() + "\n");
                //it.remove(); // avoids a ConcurrentModificationException
            }
            fw.close();
        } catch (IOException e){
            e.printStackTrace();
        }finally {
            try {
                fw.close();
            }
            catch (IOException e){
                e.printStackTrace();
            }
        }
    }
}


