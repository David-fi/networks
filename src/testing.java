import java.util.ArrayList;
import java.util.HashMap;

public class testing {
    static HashMap<Integer, ArrayList<String>> networkMap = new HashMap<>();
    static int maxNodePerDistance = 3;

    public static void main(String[] args){
        updateNetworkMap(1,"Hello");
        updateNetworkMap(1,"i");
        updateNetworkMap(1, "am");
        updateNetworkMap(1, "working");
        updateNetworkMap(1,"workin");
    }

    public static void updateNetworkMap(int distance, String node){
        try{
            ArrayList<String> values = networkMap.computeIfAbsent(distance, k -> new ArrayList<>());
            if(values.size() <= maxNodePerDistance){
                if (values.size() == maxNodePerDistance){
                    values.remove(0);
                }
                networkMap.get(distance).add(node);
                System.out.println(networkMap);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
