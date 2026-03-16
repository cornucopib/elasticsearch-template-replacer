package deepseek

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import groovy.transform.CompileStatic

import java.util.regex.Matcher
import java.util.regex.Pattern

/**
 * 高性能 Elasticsearch 查询模板替换器(豆包)
 * 优化点：
 * 1. 静态编译 (@CompileStatic) 消除 Groovy 动态开销
 * 2. 预编译正则表达式
 * 3. 使用 HashSet 替代 List 进行 O(1) 复杂度的 contains 检查
 * 4. 减少递归次数，合并重复遍历逻辑
 * 5. 使用 Java 原生 for 循环替代 Groovy 迭代器
 * 6. 优化字符串操作，避免正则替换
 * 7. 预分配集合容量，减少扩容开销
 */
@CompileStatic
class QueryTemplateReplacer2 {

    // 预编译正则表达式（关键优化：避免每次匹配都重新编译）
    private static final Pattern TEMPLATE_VARIABLE_PATTERN = Pattern.compile(/^\s*\{([^{}]+)\}\s*$/)
    private static final Pattern SCRIPT_TEMPLATE_PATTERN = Pattern.compile(/\{([^{}]+)\}/)

    // 使用 HashSet 替代 ArrayList，contains 检查从 O(n) 优化到 O(1)
    private static final Set<String> LEAF_QUERY_TYPES = new HashSet<>([
            "term", "terms", "match", "prefix", "wildcard", "regexp",
            "fuzzy", "type", "ids", "exists", "match_phrase"
    ])

    // 特殊查询类型数组（避免动态 List 分配）
    private static final String[] SPECIAL_QUERY_TYPES = ["nested", "has_child", "has_parent", "function_score"]
    private static final String[] GEO_QUERY_TYPES = ["geo_distance", "geo_bounding_box"]
    private static final String[] BOOL_CLAUSE_TYPES = ["must", "should", "must_not", "filter"]

    String processQuery(String dsl, Map<String, Object> params) {
        if (!dsl || dsl.trim().isEmpty()) return "{}"

        try {
            JSONObject queryJson = JSON.parseObject(dsl)
            processNode(queryJson, params)
            return queryJson.toJSONString()
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid DSL format: ${e.message}", e)
        }
    }

    private void processNode(JSONObject node, Map<String, Object> params) {
        // 优化：优先处理高频出现的 query 结构
        if (node.containsKey("query")) {
            def queryNode = node.get("query")
            if (queryNode instanceof JSONObject) {
                processNode((JSONObject) queryNode, params)
                if (queryNode.isEmpty()) node.remove("query")
                return
            } else if (queryNode instanceof String) {
                processStringField(node, "query", (String) queryNode, params)
                return
            }
        }

        // 优化：使用数组循环替代 each 闭包
        for (String queryType : SPECIAL_QUERY_TYPES) {
            if (node.containsKey(queryType)) {
                processSpecialQuery(node, queryType, params)
                return
            }
        }

        // 处理地理位置查询
        for (String geoType : GEO_QUERY_TYPES) {
            if (node.containsKey(geoType)) {
                handleGeoQuery(node.getJSONObject(geoType), params)
                return
            }
        }

        // 处理布尔查询
        if (node.containsKey("bool")) {
            JSONObject boolNode = node.getJSONObject("bool")
            handleBoolNode(boolNode, params)
            if (boolNode.isEmpty()) node.remove("bool")
            return
        }

        // 处理范围查询
        if (node.containsKey("range")) {
            JSONObject rangeNode = node.getJSONObject("range")
            handleRangeNode(rangeNode, params)
            if (rangeNode.isEmpty()) node.remove("range")
            return
        }

        // 处理脚本查询
        if (node.containsKey("script")) {
            JSONObject scriptNode = node.getJSONObject("script")
            processScript(scriptNode, params)
            if (scriptNode.isEmpty()) node.remove("script")
            return
        }

        // 处理叶节点
        handleLeafOrOtherNode(node, params)
    }

    private void processSpecialQuery(JSONObject node, String queryType, Map<String, Object> params) {
        JSONObject queryBody = node.getJSONObject(queryType)
        processNode(queryBody, params)
        if (queryBody.isEmpty()) node.remove(queryType)
    }

    private void processScript(JSONObject script, Map<String, Object> params) {
        if (script.containsKey("params")) {
            JSONObject scriptParams = script.getJSONObject("params")
            // 预分配容量，避免扩容
            List<String> keysToRemove = new ArrayList<>(scriptParams.size())

            // 使用 entrySet 迭代器
            for (Map.Entry<String, Object> entry : scriptParams.entrySet()) {
                if (entry.value instanceof String) {
                    String value = (String) entry.value
                    Object replaced = replaceTemplate(value, params)
                    if (replaced != null) {
                        scriptParams.put(entry.key, replaced)
                    } else if (isTemplateVariable(value)) {
                        keysToRemove.add(entry.key)
                    }
                }
            }

            for (String key : keysToRemove) {
                scriptParams.remove(key)
            }

            if (scriptParams.isEmpty()) script.remove("params")
        }
    }

    // 优化：脚本源代码替换（使用预编译 Pattern）
    private String replaceAllTemplatesInScript(String source, Map<String, Object> params) {
        Matcher matcher = SCRIPT_TEMPLATE_PATTERN.matcher(source)
        StringBuffer result = new StringBuffer(source.length()) // 预分配容量

        while (matcher.find()) {
            String varName = matcher.group(1).trim()
            if (params.containsKey(varName)) {
                matcher.appendReplacement(result, Matcher.quoteReplacement(params.get(varName).toString()))
            } else {
                matcher.appendReplacement(result, Matcher.quoteReplacement(matcher.group(0)))
            }
        }
        matcher.appendTail(result)
        return result.toString()
    }

    private void handleGeoQuery(JSONObject geo, Map<String, Object> params) {
        for (Map.Entry<String, Object> entry : geo.entrySet()) {
            if (entry.value instanceof String) {
                String value = (String) entry.value
                if (value.contains(",")) {
                    // 优化：使用 split 后手动处理，避免 collect 闭包
                    String[] coords = value.split(",")
                    StringBuilder sb = new StringBuilder()
                    for (int i = 0; i < coords.length; i++) {
                        String coord = coords[i].trim()
                        Object replaced = replaceTemplate(coord, params)
                        if (replaced != null) {
                            sb.append(replaced.toString())
                        } else {
                            sb.append(coord)
                        }
                        if (i < coords.length - 1) sb.append(",")
                    }
                    geo.put(entry.key, sb.toString())
                } else {
                    Object replaced = replaceTemplate(value, params)
                    if (replaced != null) geo.put(entry.key, replaced)
                }
            }
        }
    }

    private void handleBoolNode(JSONObject boolNode, Map<String, Object> params) {
        for (String clauseType : BOOL_CLAUSE_TYPES) {
            if (!boolNode.containsKey(clauseType)) continue

            def clauses = boolNode.get(clauseType)
            if (clauses instanceof JSONArray) {
                JSONArray clauseArray = (JSONArray) clauses
                int size = clauseArray.size()
                List<Integer> indexesToRemove = new ArrayList<>(size)

                // 第一步：递归处理
                for (int i = 0; i < size; i++) {
                    def clause = clauseArray.get(i)
                    if (clause instanceof JSONObject) {
                        processNode((JSONObject) clause, params)
                    }
                }

                // 第二步：标记删除
                for (int i = 0; i < size; i++) {
                    def clause = clauseArray.get(i)
                    if (clause instanceof JSONObject && shouldRemoveClause((JSONObject) clause, params)) {
                        indexesToRemove.add(i)
                    }
                }

                // 第三步：倒序删除（优化：手动排序后删除）
                if (!indexesToRemove.isEmpty()) {
                    // 降序排序
                    indexesToRemove.sort(Collections.reverseOrder())
                    for (int idx : indexesToRemove) {
                        clauseArray.remove(idx)
                    }
                }

                if (clauseArray.isEmpty()) boolNode.remove(clauseType)
            } else if (clauses instanceof JSONObject) {
                JSONObject clauseObj = (JSONObject) clauses
                processNode(clauseObj, params)
                if (shouldRemoveClause(clauseObj, params)) {
                    boolNode.remove(clauseType)
                }
            }
        }
    }

    private void handleRangeNode(JSONObject rangeNode, Map<String, Object> params) {
        List<String> fieldsToRemove = new ArrayList<>(rangeNode.size())

        for (Map.Entry<String, Object> fieldEntry : rangeNode.entrySet()) {
            if (fieldEntry.value instanceof JSONObject) {
                JSONObject rangeConditions = (JSONObject) fieldEntry.value
                List<String> conditionsToRemove = new ArrayList<>(rangeConditions.size())

                for (Map.Entry<String, Object> condEntry : rangeConditions.entrySet()) {
                    if (condEntry.value instanceof String) {
                        Object replaced = replaceTemplate((String) condEntry.value, params)
                        if (replaced != null) {
                            rangeConditions.put(condEntry.key, replaced)
                        } else {
                            conditionsToRemove.add(condEntry.key)
                        }
                    }
                }

                for (String key : conditionsToRemove) {
                    rangeConditions.remove(key)
                }

                if (rangeConditions.isEmpty()) {
                    fieldsToRemove.add(fieldEntry.key)
                }
            }
        }

        for (String field : fieldsToRemove) {
            rangeNode.remove(field)
        }
    }

    // 优化：合并两次递归为一次
    private boolean shouldRemoveClause(JSONObject clause, Map<String, Object> params) {
        if (clause.isEmpty()) return true
        return hasUnresolvedTemplate(clause, params)
    }

    // 优化：一次性递归检查是否包含未解析模板（消除重复遍历）
    private boolean hasUnresolvedTemplate(Object obj, Map<String, Object> params) {
        if (obj instanceof String) {
            String s = (String) obj
            Matcher matcher = TEMPLATE_VARIABLE_PATTERN.matcher(s)
            if (matcher.matches()) {
                String varName = matcher.group(1).trim()
                return !params.containsKey(varName)
            }
            return false
        } else if (obj instanceof JSONObject) {
            JSONObject jsonObj = (JSONObject) obj
            for (Object value : jsonObj.values()) {
                if (hasUnresolvedTemplate(value, params)) return true
            }
            return false
        } else if (obj instanceof JSONArray) {
            JSONArray array = (JSONArray) obj
            int size = array.size()
            for (int i = 0; i < size; i++) {
                if (hasUnresolvedTemplate(array.get(i), params)) return true
            }
            return false
        }
        return false
    }

    private void handleLeafOrOtherNode(JSONObject node, Map<String, Object> params) {
        List<String> keysToRemove = new ArrayList<>(node.size())

        for (Map.Entry<String, Object> entry : node.entrySet()) {
            String key = entry.key
            Object value = entry.value

            if (LEAF_QUERY_TYPES.contains(key) && value instanceof JSONObject) {
                JSONObject queryContent = (JSONObject) value
                if (hasUnresolvedTemplate(queryContent, params)) {
                    keysToRemove.add(key)
                } else {
                    replaceTemplates(queryContent, params)
                }
            } else if (value instanceof JSONObject) {
                processNode((JSONObject) value, params)
            } else if (value instanceof JSONArray) {
                processArray((JSONArray) value, params)
            } else if (value instanceof String) {
                processStringField(node, key, (String) value, params, keysToRemove)
            }
        }

        for (String key : keysToRemove) {
            node.remove(key)
        }
    }

    // 优化：提取公共字符串处理逻辑
    private void processStringField(JSONObject node, String key, String value, Map<String, Object> params, List<String> keysToRemove = null) {
        Object replaced = replaceTemplate(value, params)
        if (replaced != null) {
            node.put(key, replaced)
        } else if (isTemplateVariable(value) && keysToRemove != null) {
            keysToRemove.add(key)
        }
    }

    private void processArray(JSONArray array, Map<String, Object> params) {
        int size = array.size()
        // 优化：从后往前遍历，避免索引错乱
        for (int i = size - 1; i >= 0; i--) {
            def element = array.get(i)
            if (element instanceof JSONObject) {
                processNode((JSONObject) element, params)
                if (((JSONObject) element).isEmpty()) {
                    array.remove(i)
                }
            } else if (element instanceof JSONArray) {
                processArray((JSONArray) element, params)
            } else if (element instanceof String) {
                Object replaced = replaceTemplate((String) element, params)
                if (replaced != null) {
                    array.set(i, replaced)
                } else if (isTemplateVariable((String) element)) {
                    array.remove(i)
                }
            }
        }
    }

    private void replaceTemplates(Object obj, Map<String, Object> params) {
        if (obj instanceof JSONObject) {
            JSONObject jsonObj = (JSONObject) obj
            for (Map.Entry<String, Object> entry : jsonObj.entrySet()) {
                Object value = entry.value
                if (value instanceof String) {
                    Object replaced = replaceTemplate((String) value, params)
                    if (replaced != null) {
                        jsonObj.put(entry.key, replaced)
                    }
                } else if (value instanceof JSONArray) {
                    processArray((JSONArray) value, params)
                } else {
                    replaceTemplates(value, params)
                }
            }
        } else if (obj instanceof JSONArray) {
            processArray((JSONArray) obj, params)
        }
    }

    // 优化：使用预编译 Matcher 进行模板匹配
    private Object replaceTemplate(String template, Map<String, Object> params) {
        Matcher matcher = TEMPLATE_VARIABLE_PATTERN.matcher(template)
        if (!matcher.matches()) return null

        String varName = matcher.group(1).trim()
        return params.containsKey(varName) ? params.get(varName) : null
    }

    // 优化：快速判断模板变量
    private static boolean isTemplateVariable(String str) {
        if (str == null) return false
        return TEMPLATE_VARIABLE_PATTERN.matcher(str).matches()
    }

    // 优化：使用 Matcher 提取变量名（替代 replaceAll）
    private static String extractVarName(String str) {
        Matcher matcher = TEMPLATE_VARIABLE_PATTERN.matcher(str)
        if (matcher.matches()) {
            return matcher.group(1).trim()
        }
        return str.trim()
    }
}