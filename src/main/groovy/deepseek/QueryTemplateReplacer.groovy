package deepseek

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject

class QueryTemplateReplacer {

    private static final List<String> COMPLEX_QUERY_TYPES = ["bool", "range", "nested"]
    private static final List<String> LEAF_QUERY_TYPES = [
            "term", "match", "prefix", "wildcard", "regexp",
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
            if (node.getJSONObject("bool").isEmpty()) {
                node.remove("bool")
            }
        } else if (node.containsKey("range")) {
            handleRangeNode(node.getJSONObject("range"), params)
            if (node.getJSONObject("range").isEmpty()) {
                node.remove("range")
            }
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
                    JSONArray clauseArray = (JSONArray) clauses

                    // 先递归处理所有子句
                    clauseArray.eachWithIndex { clause, index ->
                        if (clause instanceof JSONObject) {
                            processNode((JSONObject) clause, params)
                        }
                    }

                    // 然后检查哪些子句需要删除
                    clauseArray.eachWithIndex { clause, index ->
                        if (clause instanceof JSONObject) {
                            if (shouldRemoveClause((JSONObject) clause, params)) {
                                indexesToRemove.add(index)
                            }
                        }
                    }

                    // 逆序删除避免索引变化
                    indexesToRemove.reverseEach { clauseArray.remove(it as int) }

                    // 删除空子句
                    if (clauseArray.isEmpty()) {
                        boolNode.remove(clauseType)
                    }
                } else if (clauses instanceof JSONObject) {
                    // 处理单个对象形式的子句
                    JSONObject clauseObj = (JSONObject) clauses
                    processNode(clauseObj, params)

                    if (shouldRemoveClause(clauseObj, params)) {
                        boolNode.remove(clauseType)
                    }
                }
            }
        }
    }

    private void handleRangeNode(JSONObject rangeNode, Map<String, Object> params) {
        // 处理range查询的特殊逻辑
        List<String> fieldsToRemove = []

        rangeNode.each { String fieldName, fieldValue ->
            if (fieldValue instanceof JSONObject) {
                JSONObject rangeConditions = (JSONObject) fieldValue
                List<String> conditionsToRemove = []

                rangeConditions.each { String condition, value ->
                    if (value instanceof String && isTemplateVariable(value)) {
                        String varName = extractVarName(value)
                        if (params.containsKey(varName)) {
                            // 替换模板变量并保留值类型
                            Object paramValue = params.get(varName)
                            rangeConditions.put(condition, paramValue)
                        } else {
                            conditionsToRemove.add(condition)
                        }
                    }
                }

                conditionsToRemove.each { rangeConditions.remove(it) }

                // 如果字段的所有条件都被移除，则移除整个字段
                if (rangeConditions.isEmpty()) {
                    fieldsToRemove.add(fieldName)
                }
            }
        }

        fieldsToRemove.each { rangeNode.remove(it) }
    }

    private boolean shouldRemoveClause(JSONObject clause, Map<String, Object> params) {
        // 空节点直接删除
        if (clause.isEmpty()) return true

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
            } else if (value instanceof JSONArray) {
                // 处理数组中的嵌套对象
                ((JSONArray) value).each {
                    if (it instanceof JSONObject) {
                        processNode((JSONObject) it, params)
                    }
                }
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
                            // 保留参数的原生类型（数字、布尔值等）
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