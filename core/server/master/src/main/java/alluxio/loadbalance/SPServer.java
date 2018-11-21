package alluxio.loadbalance;
import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.ReadType;
import alluxio.client.WriteType;
import alluxio.client.file.*;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.client.file.policy.LocalFirstPolicy;
import alluxio.client.file.policy.RandomUniquePolicy;
import alluxio.clock.SystemClock;
import alluxio.collections.Pair;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.WorkerClient;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.RepartitionThread;
import alluxio.security.authorization.Mode;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.InodeTree;
import alluxio.thrift.FileInfo;
import alluxio.util.CommonUtils;
import alluxio.wire.BlockInfo;
import alluxio.thrift.FileSystemWorkerClientService;
import alluxio.wire.WorkerInfo;
import com.google.common.base.*;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.Map.Entry;


/**
 * Created by yyuau on 25/12/2017.
 */
public class SPServer {
    private static final Logger LOG = LoggerFactory.getLogger(SPServer.class);

    ConcurrentHashMap<AlluxioURI, Long> mAccessHistory = new ConcurrentHashMap<>(); // thread safe
    public boolean changeFlag = false; //  Avoid duplicate logging: set this flag when the accessHistory has been changed.
    // Only output the history when the flag is set.

    // private Long mTotalCount;
    // private Long mThreshold = 10L;
    String mLog;
    private SPMasterLogger mLogger;
    private Timer mTimer = new Timer();
    private DefaultFileSystemMaster mMaster;
    private String KValuePath = System.getProperty("user.home") + "/test_files/k.txt"; // k values
    private String ResultsPath = System.getProperty("user.home") + "/test_files/rep.txt"; // exp results for repartition
    private List<Integer> mKValues = new ArrayList<>();
    //private Queue<WorkerLoad> mLoads = new PriorityQueue<WorkerLoad>(); // always find the one with the least load
    private Map<Long, Integer> mLoads = new HashMap<Long, Integer>(); // worker id -> partition #
    private List<Long> mWorkerIds = new ArrayList<Long>(); // used for mapping from worker indices kept by Alluxio to worker ids.
    // for parallel repartition
    private Map<Long, WorkerClient> mWorkerClients= new HashMap<Long, WorkerClient>(); // worker id -> worker client
    //private boolean initializedK = false; // whether the origin k values have been recorded


    public SPServer(DefaultFileSystemMaster master) {
        // mTotalCount = 0L; // log the access history every mThreshold accesses
        mTimer = new Timer();
        mLogger = new SPMasterLogger(this);
        mTimer.schedule(mLogger, 0, 10000); // log the access history every 10 secs
        String logDirPath = System.getProperty("user.dir") + "/logs";
        File LogDir = new File(logDirPath);
        if (!LogDir.exists()){
            LogDir.mkdir();
        }
        mMaster = master;
        mLog = logDirPath + "/SPServer.txt"; // log the popularity
        System.out.println("File path is" + mLog);
    }

    public void onAccess(AlluxioURI alluxioURI) {
        // mTotalCount += 1;
        mAccessHistory.putIfAbsent(alluxioURI, 0L); // atomic operation
        mAccessHistory.put(alluxioURI, mAccessHistory.get(alluxioURI)+1);
        if(!changeFlag) {
            changeFlag = true;
        }

        /* the if-else statement is not atomic
        if (mAccessHistory.containsKey(alluxioURI)) {
            mAccessHistory.put(alluxioURI, mAccessHistory.get(alluxioURI)+1);
        }
        else
            mAccessHistory.put(alluxioURI, 1L);
        // if(mTotalCount % mThreshold == 0) {
           // doLog();
        // }
         */
    }

    public void onCreate(AlluxioURI alluxioURI) {
        mAccessHistory.put(alluxioURI, 0L);
    }

    public void getKValues(){
        mKValues.clear();
        try {
            BufferedReader br = new BufferedReader(new FileReader(KValuePath));
            String line;
            while ((line = br.readLine()) != null) {
                mKValues.add(Integer.parseInt(line));
            }
            br.close();
        }catch(Exception e){
            e.printStackTrace();
        }
        LOG.info("K values updated" + mKValues.toString());
    }

    //repartition the files in parallel (distribute the loads across many workers)
    public void repartition(){
        // get worker clients
        mWorkerClients.clear();
        mLoads.clear();
        for(int index = 0; index < mMaster.mBlockMaster.getWorkerInfoList().size();index++){
            WorkerInfo info= mMaster.mBlockMaster.getWorkerInfoList().get(index);
            long workerId = info.getId();
            mWorkerClients.put(workerId, new WorkerClient(null, new InetSocketAddress(info.getAddress().getHost(),info.getAddress().getRpcPort()), workerId));
            mLoads.put(workerId,0);
            mWorkerIds.add(workerId);
        }
        int workerCount = mLoads.size();
        LOG.info("Worker count: " + workerCount);
        // mWorkerClients.get(0).repartition();

        // get the k values
        List<Integer> preKValues= new ArrayList<Integer>(mKValues);
        getKValues();

        // For files whose partition # is not changed, keep their partitions unmoved.
        // Count the partition number on each worker
        final long startTimeMs = CommonUtils.getCurrentMs();
        for(int fileId = 0; fileId<mKValues.size();fileId++){
            if(mKValues.get(fileId).equals(preKValues.get(fileId))){
                LOG.info("No-rep: File" + fileId);
                try (LockedInodePath inodePath = mMaster.mInodeTree.lockFullInodePath(new AlluxioURI("/tests/"+fileId), InodeTree.LockMode.READ)) {
                    mMaster.mPermissionChecker.checkPermission(Mode.Bits.READ, inodePath);
                    List<Long> blockIds = inodePath.getInodeFile().getBlockIds();
                    List<BlockInfo> blockInfos = mMaster.mBlockMaster.getBlockInfoList(blockIds);
                    for(BlockInfo info: blockInfos){
                        long workerId = info.getLocations().get(0).getWorkerId();
                        if(mLoads.containsKey(workerId))
                            mLoads.put(workerId,mLoads.get(workerId)+1);
                        else
                            mLoads.put(workerId,1);
                        LOG.info("Worker"+ workerId + "has now " + mLoads.get(workerId)+ " partitions.");
                    }
                }catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }

        // List<Pair<Integer, List<Long>>> repartitionPlan= new ArrayList<Pair<Integer, List<Long>>>();
        //For files to be repartitioned, put their partitions on the workers with the least loads.
        // assign the repartition task at one of the locations//via worker clients one by one.
        ExecutorService executorService = Executors.newCachedThreadPool();
        // int workerIndex = 0;
        int repCount = 0; // # of files to be repartitioned
        for(int fileId = 0; fileId<mKValues.size();fileId++) {
            if (!mKValues.get(fileId).equals(preKValues.get(fileId))) {
                LOG.info("Rep: File" + fileId + " previous " + preKValues.get(fileId) + " new " + mKValues.get(fileId));
                // Pair p = new Pair(fileId, new ArrayList<>());
                repCount++;
                // List<Long> locations = new ArrayList<Long>();
                List<Integer> workerIndices = new ArrayList<Integer>();
                // sort the map by value
                List<Entry<Long, Integer>> list = new ArrayList<Entry<Long, Integer>>(mLoads.entrySet());
                Collections.sort(list, new Comparator<Entry<Long, Integer>>() {
                    public int compare(Entry<Long, Integer> o1,
                                       Entry<Long, Integer> o2) {
                        return o1.getValue().compareTo(o2.getValue()); // ascending?
                    }
                });
                for(int index = 0; index<mKValues.get(fileId);index++){
                    Entry<Long, Integer> entry = list.get(index % mLoads.size()); // in case k is larger than worker #
                    //mLoads.remove(entry.getKey());
                    mLoads.put(entry.getKey(),mLoads.get(entry.getKey())+1);
                    //locations.add(entry.getKey());
                    workerIndices.add(mWorkerIds.indexOf(entry.getKey()));
                    LOG.info("Worker"+ entry.getKey() + " will have " + mLoads.get(entry.getKey())+ " partitions.");
                }
                int randomNum = ThreadLocalRandom.current().nextInt(0, workerIndices.size());
                long workerId = mWorkerIds.get(workerIndices.get(randomNum));
                WorkerClient client = mWorkerClients.get(workerId);
                LOG.info("Worker " + workerId + "(index: " + workerIndices.get(randomNum) + ")" + " is chosen to do the repartition.");
                executorService.submit(new RepartitionThread(client, fileId, new ArrayList<Integer>(workerIndices)));
                LOG.info("Repartition request sent out via SP-Server.");
                //repartitionPlan.add(new Pair<Integer, List<Long>>(fileId, locations));
            }
        }
        //Wait till all repartition tasks are done
        executorService.shutdown();
        try {
            executorService.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        final long endTimeMs = CommonUtils.getCurrentMs();
        LOG.info("Repartition + " + repCount + " out of " + mKValues.size() + " takes " + String.valueOf(endTimeMs-startTimeMs) + " ms.");
        try{
            BufferedWriter writer = new BufferedWriter(new FileWriter(ResultsPath,true));
            writer.write(String.format("Parallel:\t %s \t %s \t %s \n", repCount, mKValues.size(), endTimeMs-startTimeMs));
            logLoads(writer);
            writer.close();
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    //repartition the files sequentially on master node
    public void repartitionSeq(){
        // get worker clients
        mWorkerClients.clear();
        mLoads.clear();
        for(int index = 0; index < mMaster.mBlockMaster.getWorkerInfoList().size();index++){
            WorkerInfo info= mMaster.mBlockMaster.getWorkerInfoList().get(index);
            long workerId = info.getId();
            mWorkerClients.put(workerId, new WorkerClient(null, new InetSocketAddress(info.getAddress().getHost(),info.getAddress().getRpcPort()), workerId));
            mLoads.put(workerId,0);
            mWorkerIds.add(workerId);
        }
        int workerCount = mLoads.size();
        LOG.info("Worker count: " + workerCount);

        // get the k values
        List<Integer> preKValues= new ArrayList<Integer>(mKValues);
        getKValues();
        final long startTimeMs = CommonUtils.getCurrentMs();
        FileSystem fs = FileSystem.Factory.get();
        for(int fileId = 0; fileId<mKValues.size();fileId++) {
            LOG.info("Sequential rep: File " + fileId + " previous " + preKValues.get(fileId) + " new " + mKValues.get(fileId));
            try {
                // Read the original file
                AlluxioURI fileUri = new AlluxioURI(String.format("/tests/%s", fileId));
                OpenFileOptions readOption = OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
                FileInStream is = fs.openFile(fileUri, readOption);
                byte[] buf =  new byte[(int)is.mFileLength];
                int b = is.read(buf);
                is.close();
                // Remove the original file
                fs.delete(fileUri);
                // Rewrite
                int k = mKValues.get(fileId);
                int blockSize = (int)Math.ceil((double)(buf.length)/k);
                CreateFileOptions writeOption = alluxio.client.file.options.CreateFileOptions.defaults().setWriteType(WriteType.MUST_CACHE)
                        .setBlockSizeBytes(blockSize)
                        .setLocationPolicy(new RandomUniquePolicy())
                        .setKValueForSP(k);
                FileOutStream os = fs.createFile(fileUri, writeOption);
                os.write(buf);
                os.close();
            }catch (Exception e){
                e.printStackTrace();
            }
            // Log the loads
            try (LockedInodePath inodePath = mMaster.mInodeTree.lockFullInodePath(new AlluxioURI("/tests/"+fileId), InodeTree.LockMode.READ)) {
                mMaster.mPermissionChecker.checkPermission(Mode.Bits.READ, inodePath);
                List<Long> blockIds = inodePath.getInodeFile().getBlockIds();
                System.out.println("BlockInfo length: " + blockIds.size());
                List<BlockInfo> blockInfos = mMaster.mBlockMaster.getBlockInfoList(blockIds);
                System.out.println("BlockInfo length: " + blockIds.size());
                for(BlockInfo info: blockInfos){
                    long workerId = info.getLocations().get(0).getWorkerId();
                    if(mLoads.containsKey(workerId))
                        mLoads.put(workerId,mLoads.get(workerId)+1);
                    else
                        mLoads.put(workerId,1);
                    LOG.info("Worker"+ workerId + "has now " + mLoads.get(workerId)+ " partitions.");
                }
            }catch(Exception e) {
                e.printStackTrace();
            }
        }
        final long endTimeMs = CommonUtils.getCurrentMs();
        LOG.info("Sequentially repartition "+  mKValues.size() + " files takes " + String.valueOf(endTimeMs-startTimeMs) + " ms.");
        try{
            BufferedWriter writer = new BufferedWriter(new FileWriter(ResultsPath,true));
            writer.write(String.format("Sequential:\t %s \t %s \n", mKValues.size(),endTimeMs-startTimeMs));
            logLoads(writer);
            writer.close();
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    private void logLoads(BufferedWriter writer){
        try {
            for (int load : mLoads.values())
                writer.write(String.format("%s\t", load));
            writer.write("\n");
        }catch(IOException e){
            e.printStackTrace();
        }
    }
}


class SPMasterLogger extends TimerTask {

    private SPServer mSPServer;

    SPMasterLogger(SPServer spServer) {
        mSPServer = spServer;
    }


    public void run() { // periodically log the access counts
        if(!mSPServer.changeFlag){
            // System.out.print("Nothing changed");
            return;
        }
        else{
            System.out.print("Flag set \n" + mSPServer.changeFlag);
            Iterator it = mSPServer.mAccessHistory.entrySet().iterator();
            FileWriter fw = null;
            try {
                fw = new FileWriter(mSPServer.mLog, true); //the true will append the new data
//            fw.write("Entry: \n");
                while (it.hasNext()) {
                    Map.Entry pair = (Map.Entry) it.next();
                    fw.write(pair.getKey() + " = " + pair.getValue() + "\n");
                    // it.remove(); // avoids a ConcurrentModificationException
                }
                fw.write("\n");
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
            mSPServer.changeFlag =false; // reset the flag
        }

    }

}


