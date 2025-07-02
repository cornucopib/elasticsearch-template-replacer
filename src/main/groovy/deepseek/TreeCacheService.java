package deepseek;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;
import java.util.*;

public class TreeCacheService {
    private final RedisTemplate<String, String> redisTemplate;

    public TreeCacheService(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    // 数据存储方法
    public void buildTreeCache(List<Item> items) {
        // 1. 构建父子关系映射
        Map<String, List<String>> childrenMap = new HashMap<>(); // parentId → [childId]
        Map<String, String> parentMap = new HashMap<>();        // childId → parentId
        Map<String, Integer> inDegree = new HashMap<>();       // 未处理的子节点计数

        for (Item item : items) {
            String id = item.getId();
            String parentId = item.getParentId();

            // 记录父节点映射
            parentMap.put(id, parentId);

            // 初始化入度
            inDegree.put(id, 0);

            // 构建子节点映射
            if (parentId != null && !parentId.isEmpty()) {
                childrenMap.computeIfAbsent(parentId, k -> new ArrayList<>()).add(id);
                inDegree.put(parentId, inDegree.getOrDefault(parentId, 0) + 1);
            }
        }

        // 2. 拓扑排序（从叶子到根）
        Queue<String> queue = new LinkedList<>();
        for (String id : inDegree.keySet()) {
            if (inDegree.get(id) == 0) queue.add(id); // 叶子节点入队
        }

        SetOperations<String, String> setOps = redisTemplate.opsForSet();
        while (!queue.isEmpty()) {
            String nodeId = queue.poll();
            String key = "node:" + nodeId + ":descendants";

            // 3. 创建当前节点的集合（包含自身）
            setOps.add(key, nodeId);

            // 4. 合并子节点的子孙集合
            List<String> children = childrenMap.get(nodeId);
            if (children != null && !children.isEmpty()) {
                List<String> childKeys = new ArrayList<>();
                for (String childId : children) {
                    childKeys.add("node:" + childId + ":descendants");
                }
                // 合并所有子节点集合
                setOps.unionAndStore(key, childKeys, key);
            }

            // 5. 更新父节点状态
            String parentId = parentMap.get(nodeId);
            if (parentId != null && !parentId.isEmpty()) {
                int newDegree = inDegree.get(parentId) - 1;
                inDegree.put(parentId, newDegree);
                if (newDegree == 0) queue.add(parentId); // 父节点入队
            }
        }
    }


    public Set<String> getDescendantsByIds(List<String> ids) {
        SetOperations<String, String> setOps = redisTemplate.opsForSet();
        List<String> keys = new ArrayList<>();
        for (String id : ids) {
            keys.add("node:" + id + ":descendants");
        }
        return setOps.union(keys); // 返回并集结果
    }
}