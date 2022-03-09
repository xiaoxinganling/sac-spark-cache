package task;

import entity.Job;
import entity.Partition;
import entity.Task;
import org.apache.log4j.Logger;
import simulator.RDDStageInfoManager;
import simulator.ReferenceCountManager;
import simulator.ReplacePolicy;
import task.ds.SLRCUtil;
import task.ds.SLRUUtil;
import task.ds.SMRDUtil;
import task.ds.SReplaceUtil;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class SCacheSpace {

    public final static int INF_SPACE = Integer.MAX_VALUE;

    public String sCacheSpaceId;

    private long totalSize;

    private long curSize;

    private ReplacePolicy policy;

    private SReplaceUtil sReplaceUtil;

    private Logger logger = Logger.getLogger(this.getClass());

    public SCacheSpace(long totalSize, ReplacePolicy policy) {
        sCacheSpaceId = String.valueOf(new Random().nextDouble());
        this.totalSize = totalSize;
        this.policy = policy;
        switch (policy) {
            case SLRU:
                sReplaceUtil = new SLRUUtil();
                break;
            case SLRC:
                sReplaceUtil = new SLRCUtil();
                break;
            case SMRD:
                sReplaceUtil = new SMRDUtil();
                break;
            case SDP:
                break;
            default:
        }
        curSize = 0;
        logger.info(String.format("SCacheSpace [%s]: initialize with size of [%d] and evicting policy [%s].",
                sCacheSpaceId, totalSize, policy));
    }

    public boolean addPartition(Partition partition) {
        switch (policy) {
            case SLRU:
                if (partition.getMemorySize() > totalSize) {
                    return false;
                }
                while (!getCachedPartitionIds().contains(partition.getPartitionId())
                        && totalSize - curSize < partition.getMemorySize()) {
                    Partition toDelete = sReplaceUtil.deletePartition();
                    curSize -= toDelete.getMemorySize();
                }
                boolean successfullyAdd = sReplaceUtil.addPartition(partition);
                if (successfullyAdd) {
                    curSize += partition.getMemorySize();
                }
                return successfullyAdd;
            case SLRC:
                SLRCUtil slrcUtil = (SLRCUtil) sReplaceUtil;
                // 无需添加 or 需添加但总size不够
                if (getCachedPartitionIds().contains(partition.getPartitionId()) || totalSize < partition.getMemorySize()) {
                    return false;
                }
                // 需添加，但是需删除, false表示已经无reference count
                successfullyAdd = slrcUtil.addPartition(partition);
                if (successfullyAdd) {
                    curSize += partition.getMemorySize();
                    slrcUtil.sortPartitionByRC();
                    while (curSize > totalSize) {
                        curSize -= slrcUtil.deletePartition().getMemorySize();
                    }
                }
                return true;
            case SMRD:
                SMRDUtil smrdUtil = (SMRDUtil) sReplaceUtil;
                // 无需添加 or 需添加但总size不够
                if (getCachedPartitionIds().contains(partition.getPartitionId()) || totalSize < partition.getMemorySize()) {
                    return false;
                }
                // 需添加，但是需继续删除
                successfullyAdd = smrdUtil.addPartition(partition);
                assert successfullyAdd;
                curSize += partition.getMemorySize();
                smrdUtil.sortPartitionByDistance();
                while (curSize > totalSize) {
                    curSize -= smrdUtil.deletePartition().getMemorySize();
                }
                return true;
            default:

        }
        return false;
    }

    public boolean partitionInSCacheSpace(String partitionId) {
        boolean isHit = sReplaceUtil.getCachedPartitionIds().contains(partitionId);
        sReplaceUtil.getPartition(partitionId);
        return isHit;
    }

    public Set<String> getCachedPartitionIds() {
        return sReplaceUtil.getCachedPartitionIds();
    }

    public void prepare(String newApplication, int totalSize, List<Job> jobList, Map<Long, List<Task>> stageIdToTasks, List<Partition> hotPartitions) {
        curSize = 0;
        this.totalSize = totalSize;
        sReplaceUtil.clear();
        if (policy == ReplacePolicy.SLRC) {
            SLRCUtil slrcUtil = (SLRCUtil) sReplaceUtil;
            // set rc
            slrcUtil.setHotPartitionRC(ReferenceCountManager.generateRefCountForHotPartition(jobList, hotPartitions, stageIdToTasks));
            // set pId-action-num
            slrcUtil.setPartitionIdToActionNum(ReferenceCountManager.generatePartitionIdToActionNum(jobList));
        } else if (policy == ReplacePolicy.SMRD) {
            SMRDUtil smrdUtil = (SMRDUtil) sReplaceUtil;
            smrdUtil.setPartitionToTaskIds(RDDStageInfoManager.generateDistanceForHotPartitions(stageIdToTasks, hotPartitions));
        }
        logger.info(String.format("SCacheSpace: prepare for new application [%s].", newApplication));
    }

    public void changeAfterTaskRun(Task task) {
        if (policy == ReplacePolicy.SLRC) {
            SLRCUtil slrcUtil = (SLRCUtil) sReplaceUtil;
            slrcUtil.updateHotPartitionRefCountByTask(task);
//            logger.info(String.format("SCacheSpace: update Partition Reference Count of policy [%s] after running Stage [%d] Task [%d].",
//                    policy, task.stageId, task.getTaskId()));
        } else if (policy == ReplacePolicy.SMRD) {
            SMRDUtil smrdUtil = (SMRDUtil) sReplaceUtil;
            smrdUtil.updatePartitionDistanceByTask(task);
        }
    }

}
