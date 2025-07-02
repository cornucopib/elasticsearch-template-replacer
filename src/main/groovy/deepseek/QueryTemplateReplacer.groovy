package deepseek

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject

class QueryTemplateReplacer {

    private static final List<String> COMPLEX_QUERY_TYPES = ["bool", "range", "nested"]
    private static final List<String> LEAF_QUERY_TYPES = [
            "term", "terms", "match", "prefix", "wildcard", "regexp",
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
        // 处理查询包装器
        if (node.containsKey("query")) {
            processNode(node.getJSONObject("query"), params)
            if (node.getJSONObject("query").isEmpty()) {
                node.remove("query")
            }
            return
        }

        // 处理特殊查询类型
        ["nested", "has_child", "has_parent", "function_score"].each { queryType ->
            if (node.containsKey(queryType)) {
                JSONObject queryBody = node.getJSONObject(queryType)
                processNode(queryBody, params)
                if (queryBody.isEmpty()) {
                    node.remove(queryType)
                }
                return
            }
        }

        // 处理地理查询
        if (node.containsKey("geo_distance") || node.containsKey("geo_bounding_box")) {
            handleGeoQuery(node, params)
            return
        }

        // 处理布尔查询
        if (node.containsKey("bool")) {
            handleBoolNode(node.getJSONObject("bool"), params)
            if (node.getJSONObject("bool").isEmpty()) {
                node.remove("bool")
            }
            return
        }

        // 处理范围查询
        if (node.containsKey("range")) {
            handleRangeNode(node.getJSONObject("range"), params)
            if (node.getJSONObject("range").isEmpty()) {
                node.remove("range")
            }
            return
        }

        // 处理脚本查询
        if (node.containsKey("script")) {
            processScript(node.getJSONObject("script"), params)
            return
        }

        // 默认处理
        handleLeafOrOtherNode(node, params)
    }

    private void handleGeoQuery(JSONObject node, Map<String, Object> params) {
        ["geo_distance", "geo_bounding_box"].each { geoType ->
            if (node.containsKey(geoType)) {
                JSONObject geo = node.getJSONObject(geoType)
                geo.each { field, value ->
                    if (value instanceof String) {
                        // 处理坐标点格式 "lat,lon"
                        if (value.contains(",")) {
                            String newValue = value.split(",").collect { coord ->
                                Object replaced = replaceTemplate(coord.trim(), params)
                                replaced != null ? replaced.toString() : coord
                            }.join(",")
                            geo.put(field, newValue)
                        } else {
                            Object replaced = replaceTemplate(value, params)
                            if (replaced != null) {
                                geo.put(field, replaced)
                            }
                        }
                    }
                }
            }
        }
    }

    private void processScript(JSONObject script, Map<String, Object> params) {
        // 1. 处理脚本参数
        if (script.containsKey("params")) {
            JSONObject scriptParams = script.getJSONObject("params")
            scriptParams.each { key, value ->
                if (value instanceof String) {
                    Object replaced = replaceTemplate(value, params)
                    if (replaced != null) {
                        scriptParams.put(key, replaced)
                    }
                }
            }
        }

        // 2. 处理脚本内容（增强版）
        if (script.containsKey("source")) {
            String source = script.getString("source")
            boolean modified = false

            // 处理完整模板变量 {var}
            def matcher = (source =~ /\{([^{}]+)\}/)
            while (matcher.find()) {
                String varName = matcher.group(1)
                if (params.containsKey(varName)) {
                    source = source.replace("{${varName}}", params.get(varName).toString())
                    modified = true
                }
            }

            // 处理内联变量（如 doc['price'].value * {discount}）
            matcher = (source =~ /(\{)([^{}]+)(\})/)
            while (matcher.find()) {
                String fullMatch = matcher.group(0)
                String varName = matcher.group(2)
                if (params.containsKey(varName)) {
                    source = source.replace(fullMatch, params.get(varName).toString())
                    modified = true
                }
            }

            if (modified) {
                script.put("source", source)
            }
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
        List<String> fieldsToRemove = []

        rangeNode.each { String fieldName, fieldValue ->
            if (fieldValue instanceof JSONObject) {
                JSONObject rangeConditions = (JSONObject) fieldValue
                List<String> conditionsToRemove = []

                rangeConditions.each { String condition, value ->
                    if (value instanceof String) {
                        Object replaced = replaceTemplate(value, params)
                        if (replaced != null) {
                            rangeConditions.put(condition, replaced)
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
                // 处理数组中的嵌套对象和模板变量
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
            switch (element) {
                case JSONObject:
                    processNode((JSONObject) element, params)
                    if (element.isEmpty()) {
                        array.remove(i)
                        i--
                    }
                    break
                case JSONArray:
                    processArray((JSONArray) element, params)
                    break
                case String:
                    Object replaced = replaceTemplate(element, params)
                    if (replaced != null) {
                        array.set(i, replaced)
                    } else if (isTemplateVariable(element)) {
                        array.remove(i)
                        i--
                    }
                    break
            }
        }
    }

    private void replaceTemplates(Object obj, Map<String, Object> params) {
        switch (obj) {
            case JSONObject:
                JSONObject jsonObj = (JSONObject) obj
                jsonObj.each { key, value ->
                    if (value instanceof String) {
                        Object replaced = replaceTemplate(value, params)
                        if (replaced != null) {
                            jsonObj.put(key, replaced)
                        }
                    } else if (value instanceof JSONArray) {
                        processArray((JSONArray) value, params)
                    } else {
                        replaceTemplates(value, params)
                    }
                }
                break
            case JSONArray:
                processArray((JSONArray) obj, params)
                break
        }
    }

    // 新增的模板替换方法
    private Object replaceTemplate(String template, Map<String, Object> params) {
        if (!isTemplateVariable(template)) {
            return null // 不是模板变量，返回null表示无需替换
        }

        String varName = extractVarName(template)
        return params.containsKey(varName) ? params.get(varName) : null
    }

    private static boolean isTemplateVariable(String str) {
        str ==~ /^\{[^{}]+\}$/
    }

    private static String extractVarName(String str) {
        str.substring(1, str.length() - 1)
    }
}