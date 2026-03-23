package com.cornucopib.engine

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature

import java.util.regex.Matcher
import java.util.regex.Pattern

/**
 * ES 查询模板解析引擎
 *
 * 功能：将 ES 模板 DSL JSON 中的 {varName} 变量替换为实际值
 * 支持类型安全替换、条件移除、空值清理等特性
 *
 * 所有方法均为 static，无状态，线程安全
 */
class EsQueryTemplateEngineV1 {

    // 移除标记：用于标识需要在宽松模式下移除的节点
    private static final Object REMOVE_SENTINEL = new Object()

    /** 最大递归深度限制，防止循环引用或超深嵌套导致栈溢出 */
    private static final int MAX_DEPTH = 500

    // 预编译正则：匹配所有变量占位符 {varName}（支持字母、数字、下划线、短横线、点号，首字符为字母或下划线）
    private static final Pattern VAR_PATTERN = Pattern.compile(/\{([a-zA-Z_][\w.\-]*)\}/)

    // 预编译正则：完全匹配单个变量占位符（整个字符串就是一个变量）
    private static final Pattern EXACT_VAR_PATTERN = Pattern.compile(/^\{([a-zA-Z_][\w.\-]*)\}$/)

    // ES bool 查询中的数组类型 key，空时需要清理
    private static final Set<String> ES_BOOL_ARRAY_KEYS = ['must', 'should', 'must_not', 'filter'] as Set

    // ES 查询中的对象类型 key，空时需要清理
    private static final Set<String> ES_OBJECT_KEYS = [
            'bool', 'query', 'aggs', 'aggregations', 'highlight',
            'suggest', 'sort', 'collapse', 'rescore', 'inner_hits',
            'post_filter', 'function_score'
    ] as Set

    // ES 中"空 Map 值即有效"的查询类型 key
    private static final Set<String> ES_VALID_EMPTY_KEYS = [
            'match_all', 'match_none', 'exists', 'ids',
            'geo_bounding_box', 'geo_distance', 'geo_polygon', 'geo_shape'
    ] as Set

    /**
     * 宽松模式解析：缺失变量自动移除包含该变量的父级子句
     * @param templateJson ES 模板 JSON 字符串
     * @param params 变量键值对 Map
     * @return 解析后的 ES 查询 JSON 字符串（格式化输出）
     */
    static String resolve(String templateJson, Map<String, Object> params) {
        if (params != null && !(params instanceof Map)) {
            throw new IllegalArgumentException(
                    "参数 params 必须是 Map 类型，实际为：${params.getClass().name}")
        }
        Map result = resolveToMap(templateJson, params)
        return JSON.toJSONString(result, SerializerFeature.PrettyFormat)
    }

    /**
     * 严格模式解析：缺失变量抛出 IllegalArgumentException
     * @param templateJson ES 模板 JSON 字符串
     * @param params 变量键值对 Map
     * @return 解析后的 ES 查询 JSON 字符串（格式化输出）
     * @throws IllegalArgumentException 当存在缺失变量时
     */
    static String resolveStrict(String templateJson, Map<String, Object> params) {
        if (templateJson == null || templateJson.trim().isEmpty()) {
            throw new IllegalArgumentException("模板 JSON 不能为空")
        }

        if (params != null && !(params instanceof Map)) {
            throw new IllegalArgumentException(
                    "参数 params 必须是 Map 类型，实际为：${params.getClass().name}")
        }

        // 先校验变量完整性
        List<String> missing = validate(templateJson, params)
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException("缺失变量: ${missing.join(', ')}")
        }

        Object parsedObj
        try {
            parsedObj = JSON.parse(templateJson)
        } catch (Exception e) {
            throw new IllegalArgumentException("模板 JSON 解析失败: ${e.message}", e)
        }

        if (!(parsedObj instanceof Map)) {
            throw new IllegalArgumentException(
                    "模板 JSON 必须是一个对象(Object)，不能是数组或其他类型")
        }
        Map parsed = (Map) parsedObj

        Map<String, Object> safeParams = (params ?: [:]) as Map<String, Object>
        Object result = resolveAndClean(parsed, safeParams, true, 0)

        return JSON.toJSONString(result ?: [:], SerializerFeature.PrettyFormat)
    }

    /**
     * 宽松模式解析，返回 Map 而非 JSON 字符串
     * @param templateJson ES 模板 JSON 字符串
     * @param params 变量键值对 Map
     * @return 解析后的 Map 对象
     */
    static Map resolveToMap(String templateJson, Map<String, Object> params) {
        if (templateJson == null || templateJson.trim().isEmpty()) {
            return [:]
        }

        if (params != null && !(params instanceof Map)) {
            throw new IllegalArgumentException(
                    "参数 params 必须是 Map 类型，实际为：${params.getClass().name}")
        }

        Object parsedObj
        try {
            parsedObj = JSON.parse(templateJson)
        } catch (Exception e) {
            throw new IllegalArgumentException("模板 JSON 解析失败: ${e.message}", e)
        }

        if (!(parsedObj instanceof Map)) {
            throw new IllegalArgumentException(
                    "模板 JSON 必须是一个对象(Object)，不能是数组或其他类型")
        }
        Map parsed = (Map) parsedObj

        Map<String, Object> safeParams = (params ?: [:]) as Map<String, Object>
        Object result = resolveAndClean(parsed, safeParams, false, 0)

        return (result ?: [:]) as Map
    }

    /**
     * 校验模板变量完整性
     * @param templateJson ES 模板 JSON 字符串
     * @param params 变量键值对 Map
     * @return 缺失的变量名列表（如果全部存在则返回空列表）
     */
    static List<String> validate(String templateJson, Map<String, Object> params) {
        Set<String> variables = extractVariables(templateJson)
        Map<String, Object> safeParams = params ?: [:]

        return variables.findAll { !safeParams.containsKey(it) } as List<String>
    }

    /**
     * 提取模板中所有变量名
     * @param templateJson ES 模板 JSON 字符串
     * @return 变量名集合
     */
    static Set<String> extractVariables(String templateJson) {
        if (templateJson == null || templateJson.trim().isEmpty()) {
            return [] as Set
        }

        Set<String> variables = new LinkedHashSet<>()
        Matcher matcher = VAR_PATTERN.matcher(templateJson)

        while (matcher.find()) {
            variables.add(matcher.group(1))
        }

        return variables
    }

    /**
     * 深拷贝对象（支持 Map、List、基本类型）
     * @param obj 源对象
     * @return 深拷贝后的对象
     */
    static Object deepClone(Object obj) {
        if (obj == null) {
            return null
        }

        if (obj instanceof Map) {
            Map result = new LinkedHashMap()
            ((Map) obj).each { k, v ->
                result[k] = deepClone(v)
            }
            return result
        }

        if (obj instanceof List) {
            return ((List) obj).collect { deepClone(it) }
        }

        // 基本类型和不可变对象直接返回
        // String, Number, Boolean 等都是不可变的
        return obj
    }

    // ==================== 内部方法 ====================

    /**
     * 单次遍历：同时执行变量替换和空节点清理
     * @param node 当前节点
     * @param params 变量键值对
     * @param strict 是否严格模式
     * @param depth 当前递归深度
     * @return 处理后的节点
     */
    private static Object resolveAndClean(Object node, Map<String, Object> params,
                                          boolean strict, int depth) {
        if (depth > MAX_DEPTH) {
            throw new IllegalArgumentException(
                    "递归深度超过限制(${MAX_DEPTH})，可能存在循环引用或 JSON 嵌套过深")
        }

        if (node == null) return null

        if (node instanceof Map) {
            Map source = (Map) node
            Map result = new LinkedHashMap()

            source.each { key, value ->
                // 替换 key
                Object resolvedKey = resolveString(key.toString(), params, strict)
                if (resolvedKey.is(REMOVE_SENTINEL)) return

                // 递归处理 value
                Object resolvedValue = resolveAndClean(value, params, strict, depth + 1)
                if (resolvedValue.is(REMOVE_SENTINEL)) return

                String keyStr = resolvedKey.toString()

                // 内联清理逻辑：检查 ES 特定 key 下的空结构
                boolean shouldRemove = false
                if (resolvedValue instanceof List && ES_BOOL_ARRAY_KEYS.contains(keyStr)) {
                    shouldRemove = isEffectivelyEmpty(resolvedValue)
                } else if (resolvedValue instanceof Map && ES_OBJECT_KEYS.contains(keyStr)) {
                    shouldRemove = isEffectivelyEmpty(resolvedValue)
                }

                if (!shouldRemove) {
                    result[keyStr] = resolvedValue
                }
            }
            return result
        }

        if (node instanceof List) {
            List source = (List) node
            List result = []
            source.each { item ->
                Object resolved = resolveAndClean(item, params, strict, depth + 1)
                // 过滤 REMOVE_SENTINEL 和逻辑空 Map
                if (!resolved.is(REMOVE_SENTINEL)) {
                    if (resolved instanceof Map && isEffectivelyEmpty(resolved)) {
                        return // skip 逻辑空子句如 {match:{}}
                    }
                    result.add(resolved)
                }
            }
            return result
        }

        if (node instanceof String) {
            return resolveString((String) node, params, strict)
        }

        return node  // 数字、布尔等直接返回
    }

    /**
     * 解析字符串节点
     * - 完全匹配 {var}：返回原始类型值
     * - 部分匹配：字符串插值
     */
    private static Object resolveString(String str, Map<String, Object> params, boolean strict) {
        if (str == null || str.isEmpty()) {
            return str
        }

        // 检查是否完全匹配单个变量
        Matcher exactMatcher = EXACT_VAR_PATTERN.matcher(str)
        if (exactMatcher.matches()) {
            String varName = exactMatcher.group(1)

            if (params.containsKey(varName)) {
                // 返回原始类型值（保持 int/list/bool/map 等类型）
                return params.get(varName)
            } else if (strict) {
                throw new IllegalArgumentException("缺失变量: ${varName}")
            } else {
                // 宽松模式：标记为移除
                return REMOVE_SENTINEL
            }
        }

        // 检查是否包含变量占位符
        Matcher varMatcher = VAR_PATTERN.matcher(str)
        if (!varMatcher.find()) {
            // 不包含任何变量，直接返回原字符串
            return str
        }

        // 部分匹配：字符串插值
        varMatcher.reset()
        StringBuffer sb = new StringBuffer()
        boolean allMissing = true  // 是否所有变量都缺失
        boolean anyMissing = false // 是否有变量缺失

        while (varMatcher.find()) {
            String varName = varMatcher.group(1)
            String replacement

            if (params.containsKey(varName)) {
                allMissing = false
                Object value = params.get(varName)
                replacement = value?.toString() ?: ''
            } else if (strict) {
                throw new IllegalArgumentException("缺失变量: ${varName}")
            } else {
                anyMissing = true
                // 宽松模式下，保留原始占位符
                replacement = varMatcher.group(0)
            }

            // 转义特殊字符
            varMatcher.appendReplacement(sb, Matcher.quoteReplacement(replacement))
        }
        varMatcher.appendTail(sb)

        // 宽松模式下，如果所有变量都缺失，标记为移除
        if (!strict && allMissing && anyMissing) {
            return REMOVE_SENTINEL
        }

        return sb.toString()
    }

    /**
     * 递归判断节点是否"逻辑上为空"
     * - null → 空
     * - 空 Map {} → 空
     * - Map 中所有 value 都是逻辑空 → 整个 Map 视为空（如 {match: {}} ）
     * - 空 List [] → 空
     * - List 中所有元素都是逻辑空 → 整个 List 视为空
     * - 基本类型/非空字符串 → 非空
     */
    private static boolean isEffectivelyEmpty(Object node) {
        if (node == null) return true
        if (node instanceof Map) {
            Map m = (Map) node
            if (m.isEmpty()) return true
            // 如果 Map 包含任何"空即有效"的 ES 查询 key，视为非空
            if (m.keySet().any { ES_VALID_EMPTY_KEYS.contains(it.toString()) }) return false
            return m.values().every { v -> isEffectivelyEmpty(v) }
        }
        if (node instanceof List) {
            List l = (List) node
            if (l.isEmpty()) return true
            return l.every { item -> isEffectivelyEmpty(item) }
        }
        return false
    }
}
