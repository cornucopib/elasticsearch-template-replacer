package deepseek

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import java.util.concurrent.ConcurrentHashMap

/**
 * Elasticsearch 查询模板替换器（极致性能优化版）(千问max-thinking)
 * 用于处理包含模板变量的 ES 查询 DSL，将模板变量替换为实际参数值
 * 关键优化点：
 节点处理顺序优化（基于知识库[5]中"查询语句优化"）
 优先处理不需要递归的节点类型（如query、geo等）
 优先处理布尔查询、范围查询、脚本查询等，最后处理叶节点
 这种顺序减少了不必要的递归深度
 缓存机制优化（基于知识库[5]中"缓存机制"）
 使用ConcurrentHashMap缓存模板替换结果
 避免对相同模板变量重复查找和替换
 优化了缓存命中逻辑
 递归检查优化（基于知识库[1]中"避免深度翻页"的思路）
 优化hasUnresolvedTemplateVariable方法，使用循环代替any
 减少不必要的集合创建和遍历
 字符串处理优化
 优化isTemplateVariable和extractVarName方法，避免不必要的字符串操作
 使用substring代替正则替换
 内存和性能优化（基于知识库[2]中"给Elasticsearch足够的内存"）
 减少临时对象的创建
 优化遍历逻辑，减少不必要的操作
 关键方法优化（基于知识库[4]中"查询结构优化"）
 优化replaceTemplates方法，避免不必要的递归
 优化replaceTemplate方法，提高缓存命中率
 */
class QueryTemplateReplacer3 {

    private static final Set<String> LEAF_QUERY_TYPES = new HashSet<>(Arrays.asList(
            "term", "terms", "match", "prefix", "wildcard", "regexp",
            "fuzzy", "type", "ids", "exists", "match_phrase"
    ))

    // 缓存已替换的模板变量，避免重复查找
    private static final Map<String, Object> TEMPLATE_CACHE = new ConcurrentHashMap<>()

    String processQuery(String dsl, Map<String, Object> params) {
        if (!dsl || dsl.trim().isEmpty()) return "{}"

        try {
            JSONObject queryJson = JSON.parseObject(dsl)
            processNode(queryJson, params)
            return queryJson.toJSONString()
        } catch (Exception e) {
            e.printStackTrace()
            throw new IllegalArgumentException("Invalid DSL format: ${e.message}")
        }
    }

    private void processNode(JSONObject node, Map<String, Object> params) {
        // 1. 优先处理不需要递归的节点类型
        if (node.containsKey("query")) {
            def queryNode = node.get("query")
            if (queryNode instanceof JSONObject) {
                processNode(queryNode, params)
                if (queryNode.isEmpty()) node.remove("query")
            } else if (queryNode instanceof String) {
                processStringNode(node, "query", queryNode, params)
            }
        }

        // 2. 处理特殊查询类型
        ["nested", "has_child", "has_parent", "function_score"].each { queryType ->
            if (node.containsKey(queryType)) {
                JSONObject queryBody = node.getJSONObject(queryType)
                processNode(queryBody, params)
                if (queryBody.isEmpty()) node.remove(queryType)
            }
        }

        // 3. 处理地理位置查询
        if (node.containsKey("geo_distance") || node.containsKey("geo_bounding_box")) {
            handleGeoQuery(node, params)
            return
        }

        // 4. 优先处理布尔查询
        if (node.containsKey("bool")) {
            handleBoolNode(node.getJSONObject("bool"), params)
            if (node.getJSONObject("bool").isEmpty()) node.remove("bool")
        }
        // 5. 优先处理范围查询
        else if (node.containsKey("range")) {
            handleRangeNode(node.getJSONObject("range"), params)
            if (node.getJSONObject("range").isEmpty()) node.remove("range")
        }
        // 6. 优先处理脚本查询
        else if (node.containsKey("script")) {
            processScript(node.getJSONObject("script"), params)
            if (node.getJSONObject("script").isEmpty()) node.remove("script")
        }
        // 7. 最后处理叶节点
        else {
            handleLeafOrOtherNode(node, params)
        }
    }

    private void processStringNode(JSONObject node, String key, String value, Map<String, Object> params) {
        Object replaced = replaceTemplate(value, params)
        if (replaced != null) {
            node.put(key, replaced)
        } else if (isTemplateVariable(value)) {
            node.remove(key)
        }
    }

    private void processScript(JSONObject script, Map<String, Object> params) {
        if (script.containsKey("params")) {
            JSONObject scriptParams = script.getJSONObject("params")
            List<String> keysToRemove = []
            scriptParams.forEach { key, value ->
                if (value instanceof String) {
                    Object replaced = replaceTemplate(value, params)
                    if (replaced != null) {
                        scriptParams.put(key, replaced)
                    } else if (isTemplateVariable(value)) {
                        keysToRemove.add(key)
                    }
                }
            }
            keysToRemove.each { scriptParams.remove(it) }
            if (scriptParams.isEmpty()) script.remove("params")
        }
    }

    private void handleGeoQuery(JSONObject node, Map<String, Object> params) {
        ["geo_distance", "geo_bounding_box"].each { geoType ->
            if (node.containsKey(geoType)) {
                JSONObject geo = node.getJSONObject(geoType)
                geo.forEach { field, value ->
                    if (value instanceof String) {
                        if (value.contains(",")) {
                            String newValue = value.split(",").collect { coord ->
                                String coordTrimmed = coord.trim()
                                Object replaced = replaceTemplate(coordTrimmed, params)
                                replaced != null ? replaced.toString() : coordTrimmed
                            }.join(",")
                            geo.put(field, newValue)
                        } else {
                            Object replaced = replaceTemplate(value, params)
                            if (replaced != null) geo.put(field, replaced)
                        }
                    }
                }
            }
        }
    }

    private void handleBoolNode(JSONObject boolNode, Map<String, Object> params) {
        ["must", "should", "must_not", "filter"].each { clauseType ->
            if (boolNode.containsKey(clauseType)) {
                def clauses = boolNode.get(clauseType)
                if (clauses instanceof JSONArray) {
                    removeEmptyClauses(clauses, params)
                } else if (clauses instanceof JSONObject) {
                    processNode((JSONObject) clauses, params)
                    if (shouldRemoveClause((JSONObject) clauses, params)) {
                        boolNode.remove(clauseType)
                    }
                }
            }
        }
    }

    private void removeEmptyClauses(JSONArray clauses, Map<String, Object> params) {
        List<Integer> toRemove = []
        for (int i = 0; i < clauses.size(); i++) {
            def clause = clauses.get(i)
            if (clause instanceof JSONObject) {
                processNode((JSONObject) clause, params)
                if (shouldRemoveClause((JSONObject) clause, params)) {
                    toRemove.add(i)
                }
            }
        }
        toRemove.reverse().each { clauses.remove(it) }
    }

    private boolean shouldRemoveClause(JSONObject clause, Map<String, Object> params) {
        if (clause.isEmpty()) return true
        return hasUnresolvedTemplateVariable(clause, params)
    }

    private boolean hasUnresolvedTemplateVariable(Object obj, Map<String, Object> params) {
        if (obj instanceof String) {
            return isTemplateVariable(obj) && !params.containsKey(extractVarName(obj))
        }
        if (obj instanceof JSONObject) {
            for (Object value : obj.values()) {
                if (hasUnresolvedTemplateVariable(value, params)) {
                    return true
                }
            }
            return false
        }
        if (obj instanceof JSONArray) {
            for (Object value : obj) {
                if (hasUnresolvedTemplateVariable(value, params)) {
                    return true
                }
            }
            return false
        }
        return false
    }

    private void handleLeafOrOtherNode(JSONObject node, Map<String, Object> params) {
        List<String> keysToRemove = []
        node.forEach { key, value ->
            if (LEAF_QUERY_TYPES.contains(key) && value instanceof JSONObject) {
                if (hasUnresolvedTemplateVariable(value, params)) {
                    keysToRemove.add(key)
                } else {
                    replaceTemplates(value, params)
                }
            } else if (value instanceof JSONObject) {
                processNode((JSONObject) value, params)
            } else if (value instanceof JSONArray) {
                processArray((JSONArray) value, params)
            } else if (value instanceof String) {
                Object replaced = replaceTemplate(value, params)
                if (replaced != null) {
                    node.put(key, replaced)
                } else if (isTemplateVariable(value)) {
                    keysToRemove.add(key)
                }
            }
        }
        keysToRemove.each { node.remove(it) }
    }

    private void processArray(JSONArray array, Map<String, Object> params) {
        for (int i = 0; i < array.size(); i++) {
            def element = array.get(i)
            if (element instanceof JSONObject) {
                processNode((JSONObject) element, params)
                if (element.isEmpty()) {
                    array.remove(i)
                    i--
                }
            } else if (element instanceof JSONArray) {
                processArray((JSONArray) element, params)
            } else if (element instanceof String) {
                Object replaced = replaceTemplate(element, params)
                if (replaced != null) {
                    array.set(i, replaced)
                } else if (isTemplateVariable(element)) {
                    array.remove(i)
                    i--
                }
            }
        }
    }

    private void replaceTemplates(Object obj, Map<String, Object> params) {
        if (obj instanceof JSONObject) {
            JSONObject jsonObj = (JSONObject) obj
            jsonObj.forEach { key, value ->
                if (value instanceof String) {
                    Object replaced = replaceTemplate(value, params)
                    if (replaced != null) {
                        jsonObj.put(key, replaced)
                    }
                } else if (value instanceof JSONArray) {
                    processArray((JSONArray) value, params)
                }
            }
        } else if (obj instanceof JSONArray) {
            processArray((JSONArray) obj, params)
        }
    }

    private Object replaceTemplate(String template, Map<String, Object> params) {
        if (!isTemplateVariable(template)) return null
        String varName = extractVarName(template)

        // 8. 优化缓存机制
        if (TEMPLATE_CACHE.containsKey(varName)) {
            return TEMPLATE_CACHE.get(varName)
        }

        Object result = params.get(varName)
        if (result != null) {
            TEMPLATE_CACHE.put(varName, result)
        }
        return result
    }

    private static boolean isTemplateVariable(String str) {
        if (str == null) return false
        str = str.trim()
        return str.length() >= 2 && str.startsWith("{") && str.endsWith("}")
    }

    private static String extractVarName(String str) {
        str = str.trim()
        return str.substring(1, str.length() - 1)
    }
}