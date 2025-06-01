package deepseek

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject

class QueryTemplateReplacer {

    private static final List<String> LEAF_QUERY_TYPES = [
            "term", "match", "range", "prefix", "wildcard", "regexp",
            "fuzzy", "type", "ids", "exists", "match_phrase"
    ]

    String processQuery(String dsl, Map<String, Object> params) {
        if (!dsl || dsl.trim().isEmpty()) return "{}"

        try {
            JSONObject queryJson = JSON.parseObject(dsl)
            processNode(queryJson, params)
            return queryJson.toJSONString()
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid DSL format: ${e.message}")
        }
    }

    private void processNode(JSONObject node, Map<String, Object> params) {
        if (node.containsKey("bool")) {
            handleBoolNode(node.getJSONObject("bool"), params)
        } else {
            handleLeafOrOtherNode(node, params)
        }
    }

    private void handleBoolNode(JSONObject boolNode, Map<String, Object> params) {
        ["must", "should", "must_not", "filter"].each { clauseType ->
            if (boolNode.containsKey(clauseType)) {
                def clauses = boolNode.get(clauseType)
                if (clauses instanceof JSONArray) {
                    List<Integer> indexesToRemove = []
                    clauses.eachWithIndex { clause, index ->
                        if (clause instanceof JSONObject) {
                            if (shouldRemoveClause(clause, params)) {
                                indexesToRemove.add(index)
                            } else {
                                processNode(clause, params)
                            }
                        }
                    }

                    // 逆序删除避免索引变化
                    indexesToRemove.reverseEach { clauses.remove(it as int) }

                    // 删除空子句
                    if (clauses.isEmpty()) {
                        boolNode.remove(clauseType)
                    }
                } else if (clauses instanceof JSONObject) {
                    // 处理单个对象形式的子句
                    if (shouldRemoveClause(clauses, params)) {
                        boolNode.remove(clauseType)
                    } else {
                        processNode(clauses, params)
                    }
                }
            }
        }
    }

    private boolean shouldRemoveClause(JSONObject clause, Map<String, Object> params) {
        boolean hasTemplate = containsTemplateVariable(clause)
        boolean hasUnresolvedTemplate = hasUnresolvedTemplateVariable(clause, params)

        return hasTemplate && hasUnresolvedTemplate
    }

    private boolean containsTemplateVariable(Object obj) {
        switch (obj) {
            case String:
                return isTemplateVariable((String) obj)
            case JSONObject:
                return ((JSONObject) obj).any { key, value -> containsTemplateVariable(value) }
            case JSONArray:
                return ((JSONArray) obj).any { containsTemplateVariable(it) }
            default:
                return false
        }
    }

    private boolean hasUnresolvedTemplateVariable(Object obj, Map<String, Object> params) {
        switch (obj) {
            case String:
                return isTemplateVariable((String) obj) && !params.containsKey(extractVarName((String) obj))
            case JSONObject:
                return ((JSONObject) obj).any { key, value -> hasUnresolvedTemplateVariable(value, params) }
            case JSONArray:
                return ((JSONArray) obj).any { hasUnresolvedTemplateVariable(it, params) }
            default:
                return false
        }
    }

    private void handleLeafOrOtherNode(JSONObject node, Map<String, Object> params) {
        List<String> keysToRemove = []

        node.each { String key, value ->
            if (LEAF_QUERY_TYPES.contains(key) && value instanceof JSONObject) {
                JSONObject queryContent = (JSONObject) value
                if (containsTemplateVariable(queryContent)) {
                    if (hasUnresolvedTemplateVariable(queryContent, params)) {
                        keysToRemove.add(key)
                    } else {
                        replaceTemplates(queryContent, params)
                    }
                }
            } else if (value instanceof JSONObject) {
                // 处理嵌套的非叶子节点
                processNode((JSONObject) value, params)
            }
        }

        keysToRemove.each { node.remove(it) }
    }

    private void replaceTemplates(Object obj, Map<String, Object> params) {
        switch (obj) {
            case JSONObject:
                JSONObject jsonObj = (JSONObject) obj
                jsonObj.each { key, value ->
                    if (value instanceof String && isTemplateVariable((String) value)) {
                        String varName = extractVarName((String) value)
                        if (params.containsKey(varName)) {
                            jsonObj.put(key, params.get(varName))
                        }
                    } else {
                        replaceTemplates(value, params)
                    }
                }
                break
            case JSONArray:
                ((JSONArray) obj).each { replaceTemplates(it, params) }
                break
        }
    }

    private static boolean isTemplateVariable(String str) {
        str ==~ /^\{[^{}]+\}$/
    }

    private static String extractVarName(String str) {
        str.substring(1, str.length() - 1)
    }
}