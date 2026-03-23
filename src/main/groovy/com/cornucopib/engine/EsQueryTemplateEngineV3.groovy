package com.cornucopib.engine

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature

import java.util.regex.Matcher
import java.util.regex.Pattern

/**
 * ES 查询模板解析引擎
 *
 * 【设计思路】
 * 1. JSON 树原地遍历：直接修改 FastJSON 解析后的 Map/List 结构，避免创建中间对象
 * 2. 类型安全替换：完全匹配 {var} 时保持原始类型（int/list/bool/map），而非一律转为 String
 * 3. 条件移除：宽松模式下，缺失变量的子句自动从父节点移除，实现"可选条件"语义
 * 4. 空值级联清理：子空 → 父空 → 祖空，自底向上递归清理无效的 ES 查询结构
 *
 * 【核心机制】
 * - REMOVE_SENTINEL：哨兵对象，用于宽松模式下标记"需要删除"的节点
 * - 原地修改：使用 Iterator 遍历 Map，倒序遍历 List，确保删除操作安全
 * - 快速路径：对不含变量的字符串/key 直接跳过正则匹配，提升性能
 *
 * 【线程安全】
 * 所有方法均为 static，无实例状态，多线程调用安全
 * 正则 Pattern 预编译为 static final，线程安全且零分配
 */
class EsQueryTemplateEngineV3 {

    /**
     * 移除哨兵对象：宽松模式下标记缺失变量的节点需要被删除
     *
     * 【设计原因】
     * - 为什么不用 null？普通 JSON 值也可能是 null，会产生歧义
     * - 为什么不用特殊字符串？用户数据可能恰好包含该字符串
     * - 为什么用 new Object()？每个对象实例都是唯一的，用 .is() 引用比较确保不会误判
     *
     * 【工作流程】
     * 1. resolveString 发现缺失变量 → 返回 REMOVE_SENTINEL
     * 2. 父节点收到 REMOVE_SENTINEL → 从自身移除该子节点
     * 3. 级联向上：如果父节点因此变空，祖父节点也会移除父节点
     */
    private static final Object REMOVE_SENTINEL = new Object()

    /**
     * 最大递归深度限制
     *
     * 【设计原因】
     * - 防止恶意构造的循环引用 JSON 导致栈溢出（FastJSON 默认不检测循环引用）
     * - 防止超深嵌套（如 1000 层）导致 StackOverflowError
     * - 500 层对于正常 ES 查询绰绰有余（典型查询不超过 20 层）
     *
     * 【检查位置】
     * 在 resolveAndCleanMap 和 resolveAndCleanList 入口都检查，
     * 因为 JSON 可以 Map→List→Map→List... 交替嵌套，任一入口都可能递归
     */
    private static final int MAX_DEPTH = 500

    /**
     * 通用变量匹配正则（预编译）
     * 匹配格式：{varName}，其中 varName 支持字母、数字、下划线、短横线、点号
     * 首字符必须是字母或下划线（符合标识符命名规范）
     *
     * 【为什么用 static final】
     * - Pattern.compile() 有开销，预编译后可复用，避免每次调用都编译
     * - Pattern 是线程安全的，多线程共享无问题
     * - Matcher 不是线程安全的，但每次调用都创建新 Matcher 实例
     */
    private static final Pattern VAR_PATTERN = Pattern.compile(/\{([a-zA-Z_][\w.\-]*)\}/)

    /** ThreadLocal Matcher 缓存：避免每次调用创建新 Matcher，Matcher 非线程安全，ThreadLocal 保证线程隔离 */
    private static final ThreadLocal<Matcher> VAR_MATCHER_CACHE = ThreadLocal.withInitial {
        VAR_PATTERN.matcher("")
    }

    /**
     * ES bool 查询中的数组类型 key
     * 这些 key 的值应该是数组，空数组 [] 在 ES 中无意义，应清理
     *
     * 【清理策略】
     * 只对这些特定 key 做空值清理，而非所有 List，原因：
     * - 普通数组字段（如 tags: []）可能语义上就是"空数组"
     * - 但 must: [] 在 ES bool 查询中毫无意义，反而可能报错
     */
    private static final Set<String> ES_BOOL_ARRAY_KEYS = Collections.unmodifiableSet(
            ['must', 'should', 'must_not', 'filter'] as HashSet
    )

    /**
     * ES 查询中的对象类型 key
     * 这些 key 的值应该是对象，空对象 {} 在 ES 中通常无意义
     *
     * 【级联清理效果】
     * 例如：{ bool: { must: [] } }
     * 1. must: [] 是空数组 → 移除 must
     * 2. bool: {} 变成空对象 → 移除 bool
     * 3. 如果外层 query: {} 也空了 → 继续向上移除
     */
    private static final Set<String> ES_OBJECT_KEYS = Collections.unmodifiableSet(
            ['bool', 'query', 'aggs', 'aggregations', 'highlight',
             'suggest', 'sort', 'collapse', 'rescore', 'inner_hits',
             'post_filter', 'function_score'] as HashSet
    )

    /**
     * ES 中"空 Map 即有效"的查询类型 key
     *
     * 【设计原因】
     * 这些 key 在 ES 中即使值是空对象 {} 也有实际语义：
     * - match_all: {} → 匹配所有文档（最常见）
     * - match_none: {} → 不匹配任何文档
     * - exists: { field: "xxx" } → 字段存在查询
     * - geo_* → 地理位置查询
     *
     * 【isEffectivelyEmpty 中的作用】
     * 当 Map 包含这些 key 时，即使看起来"空"，也不应被清理
     */
    private static final Set<String> ES_VALID_EMPTY_KEYS = Collections.unmodifiableSet(
            ['match_all', 'match_none', 'exists', 'ids',
             'geo_bounding_box', 'geo_distance', 'geo_polygon', 'geo_shape'] as HashSet
    )

    /**
     * 宽松模式解析：缺失变量自动移除包含该变量的父级子句
     * @param templateJson ES 模板 JSON 字符串
     * @param params 变量键值对 Map
     * @return 解析后的 ES 查询 JSON 字符串（格式化输出）
     */
    static String resolve(String templateJson, Map<String, Object> params) {
        validateParams(params)
        Map result = resolveToMap(templateJson, params)
        return JSON.toJSONString(result)
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
        validateParams(params)

        List<String> missing = validate(templateJson, params)
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException("缺失变量: ${missing.join(', ')}")
        }

        Map parsed = parseTemplateJson(templateJson)
        Map<String, Object> safeParams = (params ?: [:]) as Map<String, Object>
        resolveAndCleanMap(parsed, safeParams, true, 0)

        return JSON.toJSONString(parsed, SerializerFeature.PrettyFormat)
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
        validateParams(params)

        Map parsed = parseTemplateJson(templateJson)
        Map<String, Object> safeParams = (params ?: [:]) as Map<String, Object>
        resolveAndCleanMap(parsed, safeParams, false, 0)

        return parsed
    }

    /**
     * 校验模板变量完整性
     * @param templateJson ES 模板 JSON 字符串
     * @param params 变量键值对 Map
     * @return 缺失的变量名列表（如果全部存在则返回空列表）
     */
    static List<String> validate(String templateJson, Map<String, Object> params) {
        if (templateJson == null || templateJson.trim().isEmpty()) {
            return Collections.emptyList()
        }
        Set<String> variables = extractVariables(templateJson)
        Map<String, Object> safeParams = params ?: [:]

        List<String> missing = new ArrayList<>()
        for (String var : variables) {
            if (!safeParams.containsKey(var)) {
                missing.add(var)
            }
        }
        return missing
    }

    /**
     * 提取模板中所有变量名
     * @param templateJson ES 模板 JSON 字符串
     * @return 变量名集合
     */
    static Set<String> extractVariables(String templateJson) {
        if (templateJson == null || templateJson.trim().isEmpty()) {
            return Collections.emptySet()
        }

        Set<String> variables = new LinkedHashSet<>()

        Matcher matcher = VAR_MATCHER_CACHE.get()
        try {
            matcher.reset(templateJson)

            while (matcher.find()) {
                variables.add(matcher.group(1))
            }

            return variables
        }finally {
            matcher.reset("")
        }
    }


    // ==================== 内部方法 ====================

    /**
     * 校验 params 参数合法性
     */
    private static void validateParams(Map<String, Object> params) {
        if (params != null && !(params instanceof Map)) {
            throw new IllegalArgumentException(
                    "参数 params 必须是 Map 类型，实际为：${params.getClass().name}")
        }
    }

    /**
     * 解析模板 JSON 字符串为 Map 对象
     */
    private static Map parseTemplateJson(String templateJson) {
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
        return (Map) parsedObj
    }

    /**
     * 原地修改 Map：同时执行变量替换和空节点清理
     *
     * 【原地修改策略】
     * - 直接修改 FastJSON 解析后的 Map，避免创建新 Map 的内存开销
     * - 使用 Iterator 遍历而非 for-each，因为需要在遍历中删除元素
     * - for-each 在删除时会抛 ConcurrentModificationException
     *
     * @param source 当前 Map 节点
     * @param params 变量键值对
     * @param strict 是否严格模式
     * @param depth 当前递归深度
     */
    private static void resolveAndCleanMap(Map source, Map<String, Object> params,
                                           boolean strict, int depth) {
        // 递归深度检查（Map 入口）
        if (depth > MAX_DEPTH) {
            throw new IllegalArgumentException(
                    "递归深度超过限制(${MAX_DEPTH})，可能存在循环引用或 JSON 嵌套过深")
        }

        if (source == null || source.isEmpty()) return

        /**
         * key 替换收集器（延迟分配）
         *
         * 【为什么需要收集后统一处理】
         * - 迭代 Map 时不能直接 put 新 key，会导致 ConcurrentModificationException
         * - 用 iter.remove() 删除旧 key 是安全的
         * - 收集新 key-value，循环结束后再 putAll 添加
         *
         * 【延迟分配优化】
         * - 绝大多数 JSON key 不含 { 变量，无需创建 keyReplacements
         * - 只在真正需要时才 new LinkedHashMap<>()，减少无谓的对象分配
         */
        Map<String, Object> keyReplacements = null

        // 使用 Iterator 遍历，支持安全删除
        Iterator<Map.Entry> iter = source.entrySet().iterator()
        while (iter.hasNext()) {
            Map.Entry entry = iter.next()
            String key = entry.getKey().toString()
            Object value = entry.getValue()

            // === 处理 value ===
            /**
             * entryRemoved 标志：标记当前 entry 是否已被删除
             *
             * 【作用】
             * - value 处理时可能删除 entry（如 value 是缺失变量）
             * - 如果 entry 已删除，后续不应再处理 key 替换
             * - 避免对已删除的 entry 调用 getValue() 等操作
             */
            boolean entryRemoved = false

            if (value instanceof String) {
                // 【优化】快速路径：不含 { 的字符串不可能有变量占位符，跳过正则匹配
                if (((String) value).indexOf('{') >= 0) {
                    Object resolved = resolveString((String) value, params, strict)
                    // .is() 是引用相等比较（同 Java 的 ==），确保只有 REMOVE_SENTINEL 实例才匹配
                    if (resolved.is(REMOVE_SENTINEL)) {
                        iter.remove()
                        entryRemoved = true
                    } else {
                        entry.setValue(resolved)
                    }
                }
            } else if (value instanceof Map) {
                // 递归处理子 Map
                resolveAndCleanMap((Map) value, params, strict, depth + 1)
                // 只对 ES 特定对象 key 做空值清理
                if (ES_OBJECT_KEYS.contains(key) && isEffectivelyEmpty(value)) {
                    iter.remove()
                    entryRemoved = true
                }
            } else if (value instanceof List) {
                // 递归处理子 List
                resolveAndCleanList((List) value, params, strict, depth + 1)
                // 只对 ES bool 数组 key 做空值清理
                if (ES_BOOL_ARRAY_KEYS.contains(key) && isEffectivelyEmpty(value)) {
                    iter.remove()
                    entryRemoved = true
                }
            }

            // === 处理 key（仅在 entry 未被删除时）===
            if (!entryRemoved) {
                /**
                 * 快速过滤：99% 的 JSON key 不含 {
                 *
                 * 【优化原因】
                 * - JSON key 通常是静态字符串如 "query"、"bool"、"must" 等
                 * - 只有极少数模板会在 key 中使用变量如 "{fieldName}": xxx
                 * - indexOf('{') 是 O(n) 字符串扫描，但比创建 Matcher + 正则匹配快得多
                 * - 省去 Matcher 创建和正则匹配开销
                 */
                if (key.indexOf('{') >= 0) {
                    Object resolvedKey = resolveString(key, params, strict)
                    if (resolvedKey.is(REMOVE_SENTINEL)) {
                        iter.remove()
                    } else if (!resolvedKey.toString().equals(key)) {
                        // key 发生了变化，需要替换
                        if (keyReplacements == null) keyReplacements = new LinkedHashMap<>(4)
                        keyReplacements.put(resolvedKey.toString(), entry.getValue())
                        iter.remove()
                    }
                }
            }
        }

        // 添加替换后的 key 条目（仅在有替换时）
        if (keyReplacements != null && !keyReplacements.isEmpty()) {
            source.putAll(keyReplacements)
        }
    }

    /**
     * 原地修改 List：同时执行变量替换和空节点清理
     *
     * 【倒序遍历策略】
     * - 正序遍历时删除元素会导致索引错位：删除 i=2 后，原 i=3 变成 i=2
     * - 倒序遍历时删除元素不影响前面的索引：删除 i=5 不影响 i=0~4
     * - 这是 List 原地删除的经典模式，比 Iterator 更高效（无需创建 Iterator 对象）
     *
     * @param source 当前 List 节点
     * @param params 变量键值对
     * @param strict 是否严格模式
     * @param depth 当前递归深度
     */
    private static void resolveAndCleanList(List source, Map<String, Object> params,
                                            boolean strict, int depth) {
        // 递归深度检查（List 入口）
        if (depth > MAX_DEPTH) {
            throw new IllegalArgumentException(
                    "递归深度超过限制(${MAX_DEPTH})，可能存在循环引用或 JSON 嵌套过深")
        }

        if (source == null || source.isEmpty()) return

        // 倒序遍历：从最后一个元素向前遍历，删除时索引不会错位
        for (int i = source.size() - 1; i >= 0; i--) {
            Object item = source.get(i)

            if (item instanceof String) {
                // 【优化】快速路径：不含 { 的字符串跳过正则匹配
                if (((String) item).indexOf('{') >= 0) {
                    Object resolved = resolveString((String) item, params, strict)
                    if (resolved.is(REMOVE_SENTINEL)) {
                        source.remove(i)  // 倒序删除，索引安全
                    } else {
                        source.set(i, resolved)
                    }
                }
            } else if (item instanceof Map) {
                resolveAndCleanMap((Map) item, params, strict, depth + 1)
                // 移除逻辑空 Map（List 中的空 Map 通常无意义）
                if (isEffectivelyEmpty(item)) {
                    source.remove(i)
                }
            } else if (item instanceof List) {
                resolveAndCleanList((List) item, params, strict, depth + 1)
                // 移除空 List（嵌套空数组无意义）
                if (((List) item).isEmpty()) {
                    source.remove(i)
                }
            }
        }
    }

    /**
     * 解析字符串节点：两段式快速路径
     *
     * 【两段式设计】
     * 第一段：完全匹配检查（复用 VAR_PATTERN + matches()）→ 类型安全的核心
     *   - 字符串恰好是 "{var}" 形式
     *   - 直接返回变量的原始值，保持 int/list/bool/map 类型
     *   - 例："{count}" + params["count"]=10 → 返回 Integer(10)，而非 "10"
     *   - 【优化】matches() 要求整个字符串匹配正则，等价于 ^{var}$，无需单独的 EXACT_VAR_PATTERN
     *
     * 第二段：单次遍历完成"有无变量"判断和插值替换（合并了原来的第二段和第三段）
     *   - 【优化】原版先 find() 判断有无变量，reset() 后再 while(find()) 遍历，共 2 次扫描
     *   - 现在合并为 1 次 while(find()) 遍历，用 found 标记判断有无变量
     *   - 部分匹配时进行字符串插值，如 "prefix_{var}_suffix"
     *
     * @param str 待解析的字符串
     * @param params 变量参数
     * @param strict 是否严格模式
     * @return 解析后的值（可能是任意类型）或 REMOVE_SENTINEL
     */
    private static Object resolveString(String str, Map<String, Object> params, boolean strict) {
        if (str == null || str.isEmpty()) return str

        Matcher m = VAR_MATCHER_CACHE.get()
        try {
            m.reset(str)

            // === 第一段：完全匹配检查（复用 VAR_PATTERN + matches()）===
            // 【优化】matches() 要求整个字符串匹配正则，等价于 ^{var}$，无需单独的 EXACT_VAR_PATTERN
            if (m.matches()) {
                String varName = m.group(1)
                if (params.containsKey(varName)) {
                    return params.get(varName)
                } else if (strict) {
                    throw new IllegalArgumentException("缺失变量: ${varName}")
                } else {
                    return REMOVE_SENTINEL
                }
            }

            // === 第二段：单次遍历完成"有无变量"判断和插值替换 ===
            // 【优化】原版先 find() 判断有无变量，reset() 后再 while(find()) 遍历，共 2 次扫描
            // 现在合并为 1 次 while(find()) 遍历，用 found 标记判断有无变量
            m.reset()
            // 注：Java 8 的 Matcher.appendReplacement 不支持 StringBuilder，必须用 StringBuffer
            StringBuffer sb = new StringBuffer()
            boolean found = false
            boolean allMissing = true
            boolean anyMissing = false

            while (m.find()) {
                found = true
                String varName = m.group(1)
                String replacement

                if (params.containsKey(varName)) {
                    allMissing = false
                    Object value = params.get(varName)
                    replacement = value?.toString() ?: ''
                } else if (strict) {
                    throw new IllegalArgumentException("缺失变量: ${varName}")
                } else {
                    anyMissing = true
                    replacement = m.group(0)
                }

                m.appendReplacement(sb, Matcher.quoteReplacement(replacement))
            }

            // 无变量占位符，直接返回原字符串
            if (!found) return str

            m.appendTail(sb)

            // 宽松模式下，如果所有变量都缺失，标记整个字符串为移除
            if (!strict && allMissing && anyMissing) {
                return REMOVE_SENTINEL
            }

            return sb.toString()
        }finally {
            m.reset("")
        }
    }

    /**
     * 递归判断节点是否"逻辑上为空"
     *
     * 【"逻辑空"vs"语义有效空"】
     * - 逻辑空：数据结构为空（null、{}、[]）
     * - 语义有效空：在 ES 中空值本身有意义（如 match_all: {}）
     *
     * 【判断规则】
     * - null → 空
     * - 空 Map {} → 空（除非包含 ES_VALID_EMPTY_KEYS）
     * - Map 中所有 value 都是逻辑空 → 空（如 {match: {}}）
     * - 空 List [] → 空
     * - List 中所有元素都是逻辑空 → 空
     * - 基本类型/非空字符串 → 非空
     *
     * 【单次遍历优化】
     * 合并 key 检查和 value 检查到同一个 entrySet 循环中，
     * 避免先遍历 keySet 再遍历 values 的双重遍历开销
     */
    private static boolean isEffectivelyEmpty(Object node) {
        if (node == null) return true

        if (node instanceof Map) {
            Map m = (Map) node
            if (m.isEmpty()) return true

            // 单次遍历 entrySet：同时检查 key 白名单和 value 空值
            for (Map.Entry entry : m.entrySet()) {
                // 优先检查 key：如果是 ES 语义有效空的 key，整个 Map 视为非空
                if (ES_VALID_EMPTY_KEYS.contains(entry.getKey().toString())) {
                    return false  // 短路返回：match_all:{} 是有效查询
                }
                // 检查 value：只要有一个非空 value，整个 Map 就非空
                if (!isEffectivelyEmpty(entry.getValue())) {
                    return false
                }
            }
            return true  // 所有 value 都是空的
        }

        if (node instanceof List) {
            List l = (List) node
            if (l.isEmpty()) return true
            // 使用索引遍历，避免 Iterator 分配
            for (int i = 0; i < l.size(); i++) {
                if (!isEffectivelyEmpty(l.get(i))) {
                    return false
                }
            }
            return true
        }

        // 基本类型（String/Number/Boolean）视为非空
        return false
    }
}
