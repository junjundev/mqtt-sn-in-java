package mqttsn.gateway;

import mqttsn.gateway.utils.GWParameters;

import java.util.Hashtable;
import java.util.Iterator;

public class TopicMappingTable {

    private Hashtable<Integer, String> topicIdTable;

    public TopicMappingTable(){
        topicIdTable = new Hashtable<Integer, String>();
    }


    public void initialize() {
        Iterator<?> iter = GWParameters.getPredefTopicIdTable().keySet().iterator();
        Iterator<?> iterVal = GWParameters.getPredefTopicIdTable().values().iterator();

        Integer topicId;
        String topicName;
        while (iter.hasNext()) {
            topicId = (Integer)iter.next();
            topicName = (String)iterVal.next();
            topicIdTable.put(topicId, topicName);
        }
    }

    /**
     * @param topicId
     * @param topicName
     */
    public void assignTopicId(int topicId, String topicName) {
        topicIdTable.put(new Integer (topicId), topicName);
    }

    public String getTopicName(int topicId) {
        return (String)topicIdTable.get(new Integer(topicId));
    }

    /**
     * @param topicName
     * @return
     */
    public int getTopicId(String topicName) {
        Iterator<Integer> iter = topicIdTable.keySet().iterator();
        Iterator<String> iterVal = topicIdTable.values().iterator();
        Integer ret = new Integer(0);
        while (iter.hasNext()) {
            Integer topicId = (Integer)iter.next();
            String tname = (String)(iterVal.next());
            if(tname.equals(topicName)) {
                ret = topicId;
                break;
            }
        }
        return ret.intValue();
    }

    /**
     * @param topicId
     */
    public void removeTopicId(int topicId) {
        topicIdTable.remove(new Integer(topicId));
    }

    /**
     * @param topicName
     */
    public void removeTopicId(String topicName) {
        Iterator<Integer> iter = topicIdTable.keySet().iterator();
        Iterator<String> iterVal = topicIdTable.values().iterator();
        while (iter.hasNext()) {
            Integer topicId = (Integer)iter.next();
            String tname = (String)(iterVal.next());

            if(tname.equals(topicName) && topicId.intValue() > GWParameters.getPredfTopicIdSize()) {
                topicIdTable.remove(topicId);
                break;
            }
        }
    }
}
