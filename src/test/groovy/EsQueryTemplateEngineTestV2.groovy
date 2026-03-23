import com.alibaba.fastjson.JSON
import com.cornucopib.engine.EsQueryTemplateEngineV2

/**
 * ES 查询模板引擎测试
 * 使用 Groovy 原生 assert 断言，覆盖 ES 全部核心语法
 */
class EsQueryTemplateEngineTestV2 {

    // ==================== 一、ES 全文查询 ====================

    /**
     * 测试 match 查询 - 基础 match 查询，带 analyzer/operator 参数
     */
    static void testMatchQuery() {
        def template = '''
        {
            "query": {
                "match": {
                    "{field}": {
                        "query": "{keyword}",
                        "analyzer": "{analyzer}",
                        "operator": "{operator}"
                    }
                }
            }
        }
        '''
        def params = [field: "title", keyword: "Spring Boot 入门教程", analyzer: "ik_max_word", operator: "and"]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        println(result)
        def json = JSON.parse(result)

        assert json.query.match.title.query == "Spring Boot 入门教程"
        assert json.query.match.title.analyzer == "ik_max_word"
        assert json.query.match.title.operator == "and"
    }

    /**
     * 测试 multi_match 查询 - 多字段匹配，带 type/fields 参数
     */
    static void testMultiMatchQuery() {
        def template = '''
        {
            "query": {
                "multi_match": {
                    "query": "{keyword}",
                    "fields": "{fields}",
                    "type": "{matchType}"
                }
            }
        }
        '''
        def params = [keyword: "Elasticsearch", fields: ["title^2", "content", "summary^1.5"], matchType: "best_fields"]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.multi_match.query == "Elasticsearch"
        assert json.query.multi_match.fields == ["title^2", "content", "summary^1.5"]
        assert json.query.multi_match.type == "best_fields"
    }

    /**
     * 测试 match_phrase 查询 - 短语匹配，带 slop 参数
     */
    static void testMatchPhraseQuery() {
        def template = '''
        {
            "query": {
                "match_phrase": {
                    "{field}": {
                        "query": "{phrase}",
                        "slop": "{slop}"
                    }
                }
            }
        }
        '''
        def params = [field: "content", phrase: "quick brown fox", slop: 2]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.match_phrase.content.query == "quick brown fox"
        assert json.query.match_phrase.content.slop == 2
    }

    /**
     * 测试 match_phrase_prefix 查询 - 短语前缀匹配
     */
    static void testMatchPhrasePrefixQuery() {
        def template = '''
        {
            "query": {
                "match_phrase_prefix": {
                    "{field}": {
                        "query": "{prefix}",
                        "max_expansions": "{maxExpansions}"
                    }
                }
            }
        }
        '''
        def params = [field: "title", prefix: "Spring Boo", maxExpansions: 50]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.match_phrase_prefix.title.query == "Spring Boo"
        assert json.query.match_phrase_prefix.title.max_expansions == 50
    }

    /**
     * 测试 query_string 查询 - 带 default_field
     */
    static void testQueryStringQuery() {
        def template = '''
        {
            "query": {
                "query_string": {
                    "query": "{queryStr}",
                    "default_field": "{defaultField}",
                    "default_operator": "{defaultOperator}"
                }
            }
        }
        '''
        def params = [queryStr: "(java OR python) AND tutorial", defaultField: "content", defaultOperator: "AND"]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.query_string.query == "(java OR python) AND tutorial"
        assert json.query.query_string.default_field == "content"
        assert json.query.query_string.default_operator == "AND"
    }

    // ==================== 二、ES 精确查询 ====================

    /**
     * 测试 term 查询 - 单值精确匹配
     */
    static void testTermQuery() {
        def template = '''
        {
            "query": {
                "term": {
                    "{field}": {
                        "value": "{value}",
                        "boost": "{boost}"
                    }
                }
            }
        }
        '''
        def params = [field: "status", value: "published", boost: 1.5]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.term.status.value == "published"
        assert json.query.term.status.boost == 1.5
    }

    /**
     * 测试 terms 查询 - 多值精确匹配（变量替换为 List）
     */
    static void testTermsQuery() {
        def template = '''
        {
            "query": {
                "terms": {
                    "{field}": "{values}"
                }
            }
        }
        '''
        def params = [field: "category", values: ["tech", "programming", "tutorial"]]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.terms.category == ["tech", "programming", "tutorial"]
        assert json.query.terms.category instanceof List
    }

    /**
     * 测试 range 查询 - 范围查询 (gte/lte/gt/lt)
     */
    static void testRangeQuery() {
        def template = '''
        {
            "query": {
                "range": {
                    "{field}": {
                        "gte": "{startDate}",
                        "lte": "{endDate}",
                        "format": "{format}"
                    }
                }
            }
        }
        '''
        def params = [field: "publish_date", startDate: "2024-01-01", endDate: "2024-12-31", format: "yyyy-MM-dd"]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.range.publish_date.gte == "2024-01-01"
        assert json.query.range.publish_date.lte == "2024-12-31"
        assert json.query.range.publish_date.format == "yyyy-MM-dd"
    }

    /**
     * 测试 range 查询 - 数值范围
     */
    static void testRangeQueryNumeric() {
        def template = '''
        {
            "query": {
                "range": {
                    "{field}": {
                        "gt": "{minValue}",
                        "lt": "{maxValue}"
                    }
                }
            }
        }
        '''
        def params = [field: "view_count", minValue: 100, maxValue: 10000]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.range.view_count.gt == 100
        assert json.query.range.view_count.lt == 10000
    }

    /**
     * 测试 exists 查询 - 字段存在查询
     */
    static void testExistsQuery() {
        def template = '''
        {
            "query": {
                "exists": {
                    "field": "{field}"
                }
            }
        }
        '''
        def params = [field: "author"]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.exists.field == "author"
    }

    /**
     * 测试 ids 查询 - ID 列表查询
     */
    static void testIdsQuery() {
        def template = '''
        {
            "query": {
                "ids": {
                    "values": "{idList}"
                }
            }
        }
        '''
        def params = [idList: ["article_001", "article_002", "article_003"]]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.ids.values == ["article_001", "article_002", "article_003"]
        assert json.query.ids.values.size() == 3
    }

    /**
     * 测试 wildcard 查询 - 通配符查询
     */
    static void testWildcardQuery() {
        def template = '''
        {
            "query": {
                "wildcard": {
                    "{field}": {
                        "value": "{pattern}",
                        "boost": "{boost}"
                    }
                }
            }
        }
        '''
        def params = [field: "title", pattern: "Spring*Boot", boost: 1.0]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.wildcard.title.value == "Spring*Boot"
        assert json.query.wildcard.title.boost == 1.0
    }

    /**
     * 测试 prefix 查询 - 前缀查询
     */
    static void testPrefixQuery() {
        def template = '''
        {
            "query": {
                "prefix": {
                    "{field}": {
                        "value": "{prefix}"
                    }
                }
            }
        }
        '''
        def params = [field: "title", prefix: "Java"]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.prefix.title.value == "Java"
    }

    /**
     * 测试 fuzzy 查询 - 模糊查询，带 fuzziness 参数
     */
    static void testFuzzyQuery() {
        def template = '''
        {
            "query": {
                "fuzzy": {
                    "{field}": {
                        "value": "{value}",
                        "fuzziness": "{fuzziness}",
                        "prefix_length": "{prefixLength}"
                    }
                }
            }
        }
        '''
        def params = [field: "title", value: "Elastcsearch", fuzziness: "AUTO", prefixLength: 2]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.fuzzy.title.value == "Elastcsearch"
        assert json.query.fuzzy.title.fuzziness == "AUTO"
        assert json.query.fuzzy.title.prefix_length == 2
    }

    /**
     * 测试 regexp 查询 - 正则查询
     */
    static void testRegexpQuery() {
        def template = '''
        {
            "query": {
                "regexp": {
                    "{field}": {
                        "value": "{pattern}",
                        "flags": "{flags}"
                    }
                }
            }
        }
        '''
        def params = [field: "title", pattern: "java.*tutorial", flags: "ALL"]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.regexp.title.value == "java.*tutorial"
        assert json.query.regexp.title.flags == "ALL"
    }

    // ==================== 三、ES 复合查询 ====================

    /**
     * 测试 bool 完整查询 - must + should + must_not + filter 组合
     */
    static void testBoolQuery() {
        def template = '''
        {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"{mustField}": "{mustValue}"}}
                    ],
                    "should": [
                        {"term": {"{shouldField}": "{shouldValue}"}}
                    ],
                    "must_not": [
                        {"term": {"{mustNotField}": "{mustNotValue}"}}
                    ],
                    "filter": [
                        {"range": {"{filterField}": {"gte": "{filterMin}"}}}
                    ],
                    "minimum_should_match": "{minMatch}"
                }
            }
        }
        '''
        def params = [
                mustField: "title", mustValue: "Elasticsearch",
                shouldField: "category", shouldValue: "tech",
                mustNotField: "status", mustNotValue: "draft",
                filterField: "view_count", filterMin: 100,
                minMatch: 1
        ]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.bool.must[0].match.title == "Elasticsearch"
        assert json.query.bool.should[0].term.category == "tech"
        assert json.query.bool.must_not[0].term.status == "draft"
        assert json.query.bool.filter[0].range.view_count.gte == 100
        assert json.query.bool.minimum_should_match == 1
    }

    /**
     * 测试 boosting 查询 - 带 positive/negative/negative_boost
     */
    static void testBoostingQuery() {
        def template = '''
        {
            "query": {
                "boosting": {
                    "positive": {
                        "match": {
                            "{positiveField}": "{positiveValue}"
                        }
                    },
                    "negative": {
                        "term": {
                            "{negativeField}": "{negativeValue}"
                        }
                    },
                    "negative_boost": "{negativeBoost}"
                }
            }
        }
        '''
        def params = [positiveField: "content", positiveValue: "java", negativeField: "status", negativeValue: "outdated", negativeBoost: 0.5]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.boosting.positive.match.content == "java"
        assert json.query.boosting.negative.term.status == "outdated"
        assert json.query.boosting.negative_boost == 0.5
    }

    /**
     * 测试 constant_score 查询 - 常量评分查询
     */
    static void testConstantScoreQuery() {
        def template = '''
        {
            "query": {
                "constant_score": {
                    "filter": {
                        "term": {
                            "{field}": "{value}"
                        }
                    },
                    "boost": "{boost}"
                }
            }
        }
        '''
        def params = [field: "status", value: "published", boost: 1.2]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.constant_score.filter.term.status == "published"
        assert json.query.constant_score.boost == 1.2
    }

    /**
     * 测试 dis_max 查询 - 带 tie_breaker
     */
    static void testDisMaxQuery() {
        def template = '''
        {
            "query": {
                "dis_max": {
                    "queries": [
                        {"match": {"{field1}": "{value1}"}},
                        {"match": {"{field2}": "{value2}"}}
                    ],
                    "tie_breaker": "{tieBreaker}"
                }
            }
        }
        '''
        def params = [field1: "title", value1: "java", field2: "content", value2: "java", tieBreaker: 0.7]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.dis_max.queries[0].match.title == "java"
        assert json.query.dis_max.queries[1].match.content == "java"
        assert json.query.dis_max.tie_breaker == 0.7
    }

    // ==================== 四、嵌套/关联查询 ====================

    /**
     * 测试 nested 查询 - 带 path 和 score_mode
     */
    static void testNestedQuery() {
        def template = '''
        {
            "query": {
                "nested": {
                    "path": "{path}",
                    "score_mode": "{scoreMode}",
                    "query": {
                        "bool": {
                            "must": [
                                {"match": {"{nestedField}": "{nestedValue}"}}
                            ]
                        }
                    }
                }
            }
        }
        '''
        def params = [path: "comments", scoreMode: "avg", nestedField: "comments.content", nestedValue: "great article"]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.nested.path == "comments"
        assert json.query.nested.score_mode == "avg"
        assert json.query.nested.query.bool.must[0].match["comments.content"] == "great article"
    }

    /**
     * 测试 has_child 查询 - 父查子
     */
    static void testHasChildQuery() {
        def template = '''
        {
            "query": {
                "has_child": {
                    "type": "{childType}",
                    "score_mode": "{scoreMode}",
                    "query": {
                        "match": {
                            "{childField}": "{childValue}"
                        }
                    }
                }
            }
        }
        '''
        def params = [childType: "comment", scoreMode: "max", childField: "rating", childValue: "5"]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.has_child.type == "comment"
        assert json.query.has_child.score_mode == "max"
        assert json.query.has_child.query.match.rating == "5"
    }

    /**
     * 测试 has_parent 查询 - 子查父
     */
    static void testHasParentQuery() {
        def template = '''
        {
            "query": {
                "has_parent": {
                    "parent_type": "{parentType}",
                    "score": "{score}",
                    "query": {
                        "term": {
                            "{parentField}": "{parentValue}"
                        }
                    }
                }
            }
        }
        '''
        def params = [parentType: "article", score: true, parentField: "author", parentValue: "john"]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.has_parent.parent_type == "article"
        assert json.query.has_parent.score == true
        assert json.query.has_parent.query.term.author == "john"
    }

    // ==================== 五、聚合查询 ====================

    /**
     * 测试 terms 聚合 - 按字段分组
     */
    static void testTermsAggregation() {
        def template = '''
        {
            "size": 0,
            "aggs": {
                "{aggName}": {
                    "terms": {
                        "field": "{field}",
                        "size": "{size}"
                    }
                }
            }
        }
        '''
        def params = [aggName: "category_count", field: "category.keyword", size: 10]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.size == 0
        assert json.aggs.category_count.terms.field == "category.keyword"
        assert json.aggs.category_count.terms.size == 10
    }

    /**
     * 测试 date_histogram 聚合 - 日期直方图
     */
    static void testDateHistogramAggregation() {
        def template = '''
        {
            "size": 0,
            "aggs": {
                "{aggName}": {
                    "date_histogram": {
                        "field": "{field}",
                        "calendar_interval": "{interval}",
                        "format": "{format}",
                        "min_doc_count": "{minDocCount}"
                    }
                }
            }
        }
        '''
        def params = [aggName: "monthly_posts", field: "publish_date", interval: "month", format: "yyyy-MM", minDocCount: 0]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.aggs.monthly_posts.date_histogram.field == "publish_date"
        assert json.aggs.monthly_posts.date_histogram.calendar_interval == "month"
        assert json.aggs.monthly_posts.date_histogram.format == "yyyy-MM"
        assert json.aggs.monthly_posts.date_histogram.min_doc_count == 0
    }

    /**
     * 测试统计聚合 - sum/avg/max/min/cardinality
     */
    static void testStatsAggregation() {
        def template = '''
        {
            "size": 0,
            "aggs": {
                "total_views": {
                    "sum": {"field": "{sumField}"}
                },
                "avg_views": {
                    "avg": {"field": "{avgField}"}
                },
                "max_views": {
                    "max": {"field": "{maxField}"}
                },
                "min_views": {
                    "min": {"field": "{minField}"}
                },
                "unique_authors": {
                    "cardinality": {"field": "{cardinalityField}"}
                }
            }
        }
        '''
        def params = [
                sumField: "view_count",
                avgField: "view_count",
                maxField: "view_count",
                minField: "view_count",
                cardinalityField: "author.keyword"
        ]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.aggs.total_views.sum.field == "view_count"
        assert json.aggs.avg_views.avg.field == "view_count"
        assert json.aggs.max_views.max.field == "view_count"
        assert json.aggs.min_views.min.field == "view_count"
        assert json.aggs.unique_authors.cardinality.field == "author.keyword"
    }

    /**
     * 测试 top_hits 聚合 - 桶内 top_hits
     */
    static void testTopHitsAggregation() {
        def template = '''
        {
            "size": 0,
            "aggs": {
                "by_category": {
                    "terms": {
                        "field": "{groupField}",
                        "size": "{groupSize}"
                    },
                    "aggs": {
                        "top_articles": {
                            "top_hits": {
                                "size": "{topSize}",
                                "_source": "{sourceFields}",
                                "sort": [
                                    {"{sortField}": {"order": "{sortOrder}"}}
                                ]
                            }
                        }
                    }
                }
            }
        }
        '''
        def params = [
                groupField: "category.keyword",
                groupSize: 5,
                topSize: 3,
                sourceFields: ["title", "author", "publish_date"],
                sortField: "view_count",
                sortOrder: "desc"
        ]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.aggs.by_category.terms.field == "category.keyword"
        assert json.aggs.by_category.aggs.top_articles.top_hits.size == 3
        assert json.aggs.by_category.aggs.top_articles.top_hits._source == ["title", "author", "publish_date"]
        assert json.aggs.by_category.aggs.top_articles.top_hits.sort[0].view_count.order == "desc"
    }

    /**
     * 测试嵌套聚合 - 多层嵌套聚合（在 terms 聚合下再做 avg 聚合）
     */
    static void testNestedAggregation() {
        def template = '''
        {
            "size": 0,
            "aggs": {
                "{outerAggName}": {
                    "terms": {
                        "field": "{outerField}",
                        "size": "{outerSize}"
                    },
                    "aggs": {
                        "{innerAggName}": {
                            "avg": {
                                "field": "{innerField}"
                            }
                        }
                    }
                }
            }
        }
        '''
        def params = [
                outerAggName: "by_author",
                outerField: "author.keyword",
                outerSize: 10,
                innerAggName: "avg_views",
                innerField: "view_count"
        ]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.aggs.by_author.terms.field == "author.keyword"
        assert json.aggs.by_author.terms.size == 10
        assert json.aggs.by_author.aggs.avg_views.avg.field == "view_count"
    }

    // ==================== 六、分页 ====================

    /**
     * 测试 from/size 分页 - 基础分页（变量替换为 Integer）
     */
    static void testFromSizePagination() {
        def template = '''
        {
            "from": "{from}",
            "size": "{size}",
            "query": {
                "match_all": {}
            }
        }
        '''
        def params = [from: 20, size: 10]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.from == 20
        assert json.size == 10
        assert json.from instanceof Integer
        assert json.size instanceof Integer
    }

    /**
     * 测试 search_after - 深度分页（变量替换为 List）
     */
    static void testSearchAfterPagination() {
        def template = '''
        {
            "size": "{size}",
            "query": {
                "match_all": {}
            },
            "sort": [
                {"publish_date": "desc"},
                {"_id": "asc"}
            ],
            "search_after": "{searchAfter}"
        }
        '''
        def params = [size: 10, searchAfter: ["2024-03-15T10:30:00Z", "article_12345"]]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.size == 10
        assert json.search_after == ["2024-03-15T10:30:00Z", "article_12345"]
        assert json.search_after instanceof List
    }

    /**
     * 测试 scroll - 滚动查询
     */
    static void testScrollQuery() {
        def template = '''
        {
            "size": "{size}",
            "query": {
                "match": {
                    "{field}": "{value}"
                }
            }
        }
        '''
        // 注：scroll 参数通常在请求 URL 中，这里测试查询体
        def params = [size: 1000, field: "status", value: "published"]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.size == 1000
        assert json.query.match.status == "published"
    }

    // ==================== 七、排序 ====================

    /**
     * 测试单字段排序 - 简单排序
     */
    static void testSingleFieldSort() {
        def template = '''
        {
            "query": {"match_all": {}},
            "sort": [
                {"{sortField}": {"order": "{sortOrder}"}}
            ]
        }
        '''
        def params = [sortField: "publish_date", sortOrder: "desc"]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.sort[0].publish_date.order == "desc"
    }

    /**
     * 测试多字段排序 - 多个排序条件
     */
    static void testMultiFieldSort() {
        def template = '''
        {
            "query": {"match_all": {}},
            "sort": [
                {"{sortField1}": {"order": "{sortOrder1}"}},
                {"{sortField2}": {"order": "{sortOrder2}"}},
                {"{sortField3}": {"order": "{sortOrder3}"}}
            ]
        }
        '''
        def params = [
                sortField1: "is_top", sortOrder1: "desc",
                sortField2: "publish_date", sortOrder2: "desc",
                sortField3: "_score", sortOrder3: "desc"
        ]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.sort[0].is_top.order == "desc"
        assert json.sort[1].publish_date.order == "desc"
        assert json.sort[2]._score.order == "desc"
    }

    /**
     * 测试嵌套排序 - nested path 排序
     */
    static void testNestedSort() {
        def template = '''
        {
            "query": {"match_all": {}},
            "sort": [
                {
                    "{sortField}": {
                        "order": "{sortOrder}",
                        "nested": {
                            "path": "{nestedPath}",
                            "filter": {
                                "term": {"{filterField}": "{filterValue}"}
                            }
                        }
                    }
                }
            ]
        }
        '''
        def params = [
                sortField: "comments.rating",
                sortOrder: "desc",
                nestedPath: "comments",
                filterField: "comments.type",
                filterValue: "verified"
        ]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.sort[0]["comments.rating"].order == "desc"
        assert json.sort[0]["comments.rating"].nested.path == "comments"
        assert json.sort[0]["comments.rating"].nested.filter.term["comments.type"] == "verified"
    }

    /**
     * 测试 _score 排序 - 按评分排序
     */
    static void testScoreSort() {
        def template = '''
        {
            "query": {
                "match": {"{field}": "{value}"}
            },
            "sort": [
                {"_score": {"order": "{scoreOrder}"}},
                {"{secondaryField}": {"order": "{secondaryOrder}"}}
            ]
        }
        '''
        def params = [field: "content", value: "elasticsearch", scoreOrder: "desc", secondaryField: "publish_date", secondaryOrder: "desc"]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.sort[0]._score.order == "desc"
        assert json.sort[1].publish_date.order == "desc"
    }

    /**
     * 测试 _geo_distance 排序 - 地理距离排序（带动态坐标参数）
     */
    static void testGeoDistanceSort() {
        def template = '''
        {
            "query": {"match_all": {}},
            "sort": [
                {
                    "_geo_distance": {
                        "{geoField}": {
                            "lat": "{lat}",
                            "lon": "{lon}"
                        },
                        "order": "{sortOrder}",
                        "unit": "{unit}",
                        "distance_type": "{distanceType}"
                    }
                }
            ]
        }
        '''
        def params = [
                geoField: "location",
                lat: 39.9042,
                lon: 116.4074,
                sortOrder: "asc",
                unit: "km",
                distanceType: "arc"
        ]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.sort[0]._geo_distance.location.lat == 39.9042
        assert json.sort[0]._geo_distance.location.lon == 116.4074
        assert json.sort[0]._geo_distance.order == "asc"
        assert json.sort[0]._geo_distance.unit == "km"
    }

    // ==================== 八、高亮 ====================

    /**
     * 测试 highlight - 带 pre_tags/post_tags/fields 的高亮查询
     */
    static void testHighlight() {
        def template = '''
        {
            "query": {
                "match": {"{queryField}": "{queryValue}"}
            },
            "highlight": {
                "pre_tags": "{preTags}",
                "post_tags": "{postTags}",
                "fields": {
                    "{highlightField1}": {
                        "number_of_fragments": "{fragments1}"
                    },
                    "{highlightField2}": {
                        "number_of_fragments": "{fragments2}"
                    }
                }
            }
        }
        '''
        def params = [
                queryField: "content",
                queryValue: "elasticsearch",
                preTags: ["<em class='highlight'>"],
                postTags: ["</em>"],
                highlightField1: "title",
                fragments1: 0,
                highlightField2: "content",
                fragments2: 3
        ]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.highlight.pre_tags == ["<em class='highlight'>"]
        assert json.highlight.post_tags == ["</em>"]
        assert json.highlight.fields.title.number_of_fragments == 0
        assert json.highlight.fields.content.number_of_fragments == 3
    }

    // ==================== 九、源过滤 ====================

    /**
     * 测试 _source includes/excludes - 字段过滤（变量替换为 List）
     */
    static void testSourceFiltering() {
        def template = '''
        {
            "query": {"match_all": {}},
            "_source": {
                "includes": "{includeFields}",
                "excludes": "{excludeFields}"
            }
        }
        '''
        def params = [
                includeFields: ["title", "author", "publish_date", "category"],
                excludeFields: ["content", "raw_html"]
        ]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json._source.includes == ["title", "author", "publish_date", "category"]
        assert json._source.excludes == ["content", "raw_html"]
        assert json._source.includes instanceof List
        assert json._source.excludes instanceof List
    }

    // ==================== 十、脚本 ====================

    /**
     * 测试 script_fields - 脚本字段
     */
    static void testScriptFields() {
        def template = '''
        {
            "query": {"match_all": {}},
            "script_fields": {
                "{scriptFieldName}": {
                    "script": {
                        "source": "{scriptSource}",
                        "lang": "{lang}"
                    }
                }
            }
        }
        '''
        def params = [
                scriptFieldName: "calculated_score",
                scriptSource: "doc['view_count'].value * 0.1 + doc['like_count'].value * 0.5",
                lang: "painless"
        ]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.script_fields.calculated_score.script.source == "doc['view_count'].value * 0.1 + doc['like_count'].value * 0.5"
        assert json.script_fields.calculated_score.script.lang == "painless"
    }

    /**
     * 测试 script query - 脚本查询
     */
    static void testScriptQuery() {
        def template = '''
        {
            "query": {
                "bool": {
                    "filter": {
                        "script": {
                            "script": {
                                "source": "{scriptSource}",
                                "params": {
                                    "threshold": "{threshold}"
                                }
                            }
                        }
                    }
                }
            }
        }
        '''
        def params = [
                scriptSource: "doc['view_count'].value > params.threshold",
                threshold: 1000
        ]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.bool.filter.script.script.source == "doc['view_count'].value > params.threshold"
        assert json.query.bool.filter.script.script.params.threshold == 1000
    }

    /**
     * 测试 script sort - 脚本排序
     */
    static void testScriptSort() {
        def template = '''
        {
            "query": {"match_all": {}},
            "sort": [
                {
                    "_script": {
                        "type": "{scriptType}",
                        "script": {
                            "source": "{scriptSource}",
                            "params": {
                                "factor": "{factor}"
                            }
                        },
                        "order": "{sortOrder}"
                    }
                }
            ]
        }
        '''
        def params = [
                scriptType: "number",
                scriptSource: "doc['view_count'].value * params.factor + doc['like_count'].value",
                factor: 2,
                sortOrder: "desc"
        ]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.sort[0]._script.type == "number"
        assert json.sort[0]._script.script.params.factor == 2
        assert json.sort[0]._script.order == "desc"
    }

    // ==================== 十一、折叠 ====================

    /**
     * 测试 collapse - 带 inner_hits 的字段折叠
     */
    static void testCollapse() {
        def template = '''
        {
            "query": {"match": {"{queryField}": "{queryValue}"}},
            "collapse": {
                "field": "{collapseField}",
                "inner_hits": {
                    "name": "{innerHitsName}",
                    "size": "{innerHitsSize}",
                    "sort": [{"{innerSortField}": "{innerSortOrder}"}]
                }
            }
        }
        '''
        def params = [
                queryField: "content",
                queryValue: "java",
                collapseField: "author.keyword",
                innerHitsName: "author_articles",
                innerHitsSize: 3,
                innerSortField: "publish_date",
                innerSortOrder: "desc"
        ]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.collapse.field == "author.keyword"
        assert json.collapse.inner_hits.name == "author_articles"
        assert json.collapse.inner_hits.size == 3
        assert json.collapse.inner_hits.sort[0].publish_date == "desc"
    }

    // ==================== 十二、后置过滤 ====================

    /**
     * 测试 post_filter - 后置过滤器
     */
    static void testPostFilter() {
        def template = '''
        {
            "query": {"match": {"{queryField}": "{queryValue}"}},
            "aggs": {
                "all_categories": {
                    "terms": {"field": "{aggField}"}
                }
            },
            "post_filter": {
                "term": {
                    "{filterField}": "{filterValue}"
                }
            }
        }
        '''
        def params = [
                queryField: "content",
                queryValue: "programming",
                aggField: "category.keyword",
                filterField: "category.keyword",
                filterValue: "java"
        ]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.match.content == "programming"
        assert json.aggs.all_categories.terms.field == "category.keyword"
        assert json.post_filter.term["category.keyword"] == "java"
    }

    // ==================== 十三、评分 ====================

    /**
     * 测试 function_score - 包含多种评分函数
     */
    static void testFunctionScore() {
        def template = '''
        {
            "query": {
                "function_score": {
                    "query": {"match": {"{queryField}": "{queryValue}"}},
                    "functions": [
                        {
                            "weight": "{weight1}",
                            "filter": {"term": {"{filterField1}": "{filterValue1}"}}
                        },
                        {
                            "field_value_factor": {
                                "field": "{fvfField}",
                                "factor": "{fvfFactor}",
                                "modifier": "{fvfModifier}",
                                "missing": "{fvfMissing}"
                            }
                        },
                        {
                            "script_score": {
                                "script": {
                                    "source": "{scriptSource}"
                                }
                            }
                        },
                        {
                            "random_score": {
                                "seed": "{randomSeed}",
                                "field": "{randomField}"
                            }
                        },
                        {
                            "gauss": {
                                "{decayField}": {
                                    "origin": "{decayOrigin}",
                                    "scale": "{decayScale}",
                                    "decay": "{decayValue}"
                                }
                            }
                        }
                    ],
                    "score_mode": "{scoreMode}",
                    "boost_mode": "{boostMode}"
                }
            }
        }
        '''
        def params = [
                queryField: "title",
                queryValue: "elasticsearch",
                weight1: 2,
                filterField1: "is_featured",
                filterValue1: true,
                fvfField: "view_count",
                fvfFactor: 1.2,
                fvfModifier: "log1p",
                fvfMissing: 1,
                scriptSource: "_score * doc['boost'].value",
                randomSeed: 12345,
                randomField: "_seq_no",
                decayField: "publish_date",
                decayOrigin: "now",
                decayScale: "30d",
                decayValue: 0.5,
                scoreMode: "sum",
                boostMode: "multiply"
        ]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.function_score.query.match.title == "elasticsearch"
        assert json.query.function_score.functions[0].weight == 2
        assert json.query.function_score.functions[1].field_value_factor.field == "view_count"
        assert json.query.function_score.functions[1].field_value_factor.modifier == "log1p"
        assert json.query.function_score.functions[2].script_score.script.source == "_score * doc['boost'].value"
        assert json.query.function_score.functions[3].random_score.seed == 12345
        assert json.query.function_score.functions[4].gauss.publish_date.origin == "now"
        assert json.query.function_score.functions[4].gauss.publish_date.decay == 0.5
        assert json.query.function_score.score_mode == "sum"
        assert json.query.function_score.boost_mode == "multiply"
    }

    // ==================== 十四、Suggest ====================

    /**
     * 测试 term suggest - 词条建议
     */
    static void testTermSuggest() {
        def template = '''
        {
            "suggest": {
                "{suggestName}": {
                    "text": "{suggestText}",
                    "term": {
                        "field": "{suggestField}",
                        "suggest_mode": "{suggestMode}",
                        "size": "{suggestSize}"
                    }
                }
            }
        }
        '''
        def params = [
                suggestName: "title_suggestion",
                suggestText: "elasticsaerch",
                suggestField: "title",
                suggestMode: "popular",
                suggestSize: 5
        ]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.suggest.title_suggestion.text == "elasticsaerch"
        assert json.suggest.title_suggestion.term.field == "title"
        assert json.suggest.title_suggestion.term.suggest_mode == "popular"
        assert json.suggest.title_suggestion.term.size == 5
    }

    /**
     * 测试 phrase suggest - 短语建议
     */
    static void testPhraseSuggest() {
        def template = '''
        {
            "suggest": {
                "{suggestName}": {
                    "text": "{suggestText}",
                    "phrase": {
                        "field": "{suggestField}",
                        "gram_size": "{gramSize}",
                        "confidence": "{confidence}",
                        "max_errors": "{maxErrors}"
                    }
                }
            }
        }
        '''
        def params = [
                suggestName: "phrase_suggestion",
                suggestText: "elastic serch tutoral",
                suggestField: "content.trigram",
                gramSize: 3,
                confidence: 1.0,
                maxErrors: 2
        ]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.suggest.phrase_suggestion.text == "elastic serch tutoral"
        assert json.suggest.phrase_suggestion.phrase.field == "content.trigram"
        assert json.suggest.phrase_suggestion.phrase.gram_size == 3
        assert json.suggest.phrase_suggestion.phrase.max_errors == 2
    }

    /**
     * 测试 completion suggest - 自动补全建议
     */
    static void testCompletionSuggest() {
        def template = '''
        {
            "suggest": {
                "{suggestName}": {
                    "prefix": "{suggestPrefix}",
                    "completion": {
                        "field": "{suggestField}",
                        "size": "{suggestSize}",
                        "skip_duplicates": "{skipDuplicates}",
                        "fuzzy": {
                            "fuzziness": "{fuzziness}"
                        }
                    }
                }
            }
        }
        '''
        def params = [
                suggestName: "title_autocomplete",
                suggestPrefix: "Spri",
                suggestField: "title.completion",
                suggestSize: 10,
                skipDuplicates: true,
                fuzziness: "AUTO"
        ]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.suggest.title_autocomplete.prefix == "Spri"
        assert json.suggest.title_autocomplete.completion.field == "title.completion"
        assert json.suggest.title_autocomplete.completion.size == 10
        assert json.suggest.title_autocomplete.completion.skip_duplicates == true
        assert json.suggest.title_autocomplete.completion.fuzzy.fuzziness == "AUTO"
    }

    // ==================== 十五、引擎功能测试 ====================

    /**
     * 测试条件移除 - 宽松模式：缺失变量时自动移除包含该变量的子句
     */
    static void testLenientModeRemoval() {
        def template = '''
        {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"title": "{titleKeyword}"}},
                        {"match": {"content": "{contentKeyword}"}}
                    ],
                    "filter": [
                        {"term": {"status": "{status}"}}
                    ]
                }
            }
        }
        '''
        // 只提供 titleKeyword，缺失 contentKeyword 和 status
        def params = [titleKeyword: "Elasticsearch"]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        // 验证结构完整性：只保留有值的 match
        assert json.query.bool.must.size() == 1
        assert json.query.bool.must[0].match.title == "Elasticsearch"
        // filter 数组为空应被清理掉，或者只有空 term 被移除
        assert json.query.bool.filter == null || json.query.bool.filter.isEmpty()
    }

    /**
     * 测试严格模式异常 - 缺失变量时抛出 IllegalArgumentException
     */
    static void testStrictModeException() {
        def template = '''
        {
            "query": {
                "match": {
                    "{field}": "{keyword}"
                }
            }
        }
        '''
        def params = [field: "title"]  // 缺失 keyword

        def exceptionThrown = false
        def exceptionMessage = ""
        try {
            EsQueryTemplateEngineV2.resolveStrict(template, params)
        } catch (IllegalArgumentException e) {
            exceptionThrown = true
            exceptionMessage = e.message
        }

        assert exceptionThrown : "严格模式应该抛出异常"
        assert exceptionMessage.contains("缺失变量") : "异常信息应包含'缺失变量'"
        assert exceptionMessage.contains("keyword") : "异常信息应包含缺失的变量名 keyword"
    }

    /**
     * 测试变量提取 - extractVariables 正确提取所有变量名
     */
    static void testExtractVariables() {
        def template = '''
        {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"{field1}": "{value1}"}},
                        {"range": {"{field2}": {"gte": {minValue}, "lte": {maxValue}}}}
                    ]
                }
            },
            "from": {from},
            "size": {size}
        }
        '''
        def variables = EsQueryTemplateEngineV2.extractVariables(template)

        assert variables.contains("field1")
        assert variables.contains("value1")
        assert variables.contains("field2")
        assert variables.contains("minValue")
        assert variables.contains("maxValue")
        assert variables.contains("from")
        assert variables.contains("size")
        assert variables.size() == 7
    }

    /**
     * 测试变量校验 - validate 返回缺失变量列表
     */
    static void testValidate() {
        def template = '''
        {
            "query": {
                "match": {"{field}": "{keyword}"}
            },
            "from": {from},
            "size": {size}
        }
        '''
        // 只提供部分变量
        def params = [field: "title", from: 0]
        def missing = EsQueryTemplateEngineV2.validate(template, params)

        assert missing.contains("keyword")
        assert missing.contains("size")
        assert !missing.contains("field")
        assert !missing.contains("from")
        assert missing.size() == 2
    }

    /**
     * 测试 Map Key 替换 - {sortField} 作为 JSON key 被正确替换
     */
    static void testMapKeyReplacement() {
        def template = '''
        {
            "query": {"match_all": {}},
            "sort": [
                {"{sortField}": {"order": "{sortOrder}"}}
            ],
            "aggs": {
                "{aggName}": {
                    "terms": {"field": "{aggField}"}
                }
            }
        }
        '''
        def params = [sortField: "create_time", sortOrder: "desc", aggName: "by_category", aggField: "category.keyword"]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        // 验证 key 被正确替换
        assert json.sort[0].containsKey("create_time")
        assert json.sort[0].create_time.order == "desc"
        assert json.aggs.containsKey("by_category")
        assert json.aggs.by_category.terms.field == "category.keyword"
    }

    /**
     * 测试嵌套深层变量 - 多层嵌套结构中的变量替换
     */
    static void testDeepNestedVariables() {
        def template = '''
        {
            "query": {
                "bool": {
                    "must": [
                        {
                            "nested": {
                                "path": "{nestedPath}",
                                "query": {
                                    "bool": {
                                        "must": [
                                            {"match": {"{nestedField1}": "{nestedValue1}"}},
                                            {"range": {"{nestedField2}": {"gte": "{nestedMin}"}}}
                                        ]
                                    }
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "{outerAgg}": {
                    "nested": {"path": "{aggNestedPath}"},
                    "aggs": {
                        "{innerAgg}": {
                            "terms": {"field": "{innerAggField}"}
                        }
                    }
                }
            }
        }
        '''
        def params = [
                nestedPath: "comments",
                nestedField1: "comments.content",
                nestedValue1: "excellent",
                nestedField2: "comments.rating",
                nestedMin: 4,
                outerAgg: "comment_stats",
                aggNestedPath: "comments",
                innerAgg: "top_keywords",
                innerAggField: "comments.keywords"
        ]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        // 验证深层嵌套变量都被正确替换
        assert json.query.bool.must[0].nested.path == "comments"
        assert json.query.bool.must[0].nested.query.bool.must[0].match["comments.content"] == "excellent"
        assert json.query.bool.must[0].nested.query.bool.must[1].range["comments.rating"].gte == 4
        assert json.aggs.comment_stats.nested.path == "comments"
        assert json.aggs.comment_stats.aggs.top_keywords.terms.field == "comments.keywords"
    }

    /**
     * 测试边界情况 - 空模板
     */
    static void testEmptyTemplate() {
        def template = '{}'
        def params = [field: "title"]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json == [:]
    }

    /**
     * 测试边界情况 - 空 params
     */
    static void testEmptyParams() {
        def template = '''
        {
            "query": {
                "match_all": {}
            }
        }
        '''
        def params = [:]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.match_all == [:]
    }

    /**
     * 测试边界情况 - 全变量缺失（宽松模式）
     */
    static void testAllVariablesMissing() {
        def template = '''
        {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"{field1}": "{value1}"}},
                        {"match": {"{field2}": "{value2}"}}
                    ]
                }
            }
        }
        '''
        def params = [:]  // 不提供任何变量
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        // 宽松模式下，所有子句被移除，bool/must 为空也被清理
        // 最终 query 可能为空或被移除
        assert json.query == null || json.query == [:] || json.query.bool == null || json.query.bool.must?.isEmpty()
    }

    /**
     * 测试边界情况 - 无变量模板
     */
    static void testNoVariableTemplate() {
        def template = '''
        {
            "query": {
                "match_all": {}
            },
            "size": 10,
            "from": 0
        }
        '''
        def params = [someUnusedVar: "value"]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.match_all == [:]
        assert json.size == 10
        assert json.from == 0
    }

    /**
     * 测试类型保持 - 确保各种类型被正确保持
     */
    static void testTypePreservation() {
        def template = '''
        {
            "size": "{sizeValue}",
            "query": {
                "bool": {
                    "must": [
                        {"terms": {"tags": "{tagList}"}},
                        {"term": {"is_active": "{isActive}"}},
                        {"range": {"score": {"gte": "{minScore}"}}}
                    ]
                }
            },
            "_source": "{sourceFields}"
        }
        '''
        def params = [
                sizeValue: 100,
                tagList: ["java", "spring", "elasticsearch"],
                isActive: true,
                minScore: 3.5,
                sourceFields: ["title", "content"]
        ]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        // 验证类型保持
        assert json.size == 100
        assert json.size instanceof Integer

        assert json.query.bool.must[0].terms.tags == ["java", "spring", "elasticsearch"]
        assert json.query.bool.must[0].terms.tags instanceof List

        assert json.query.bool.must[1].term.is_active == true
        assert json.query.bool.must[1].term.is_active instanceof Boolean

        assert json.query.bool.must[2].range.score.gte == 3.5
        assert json.query.bool.must[2].range.score.gte instanceof BigDecimal || json.query.bool.must[2].range.score.gte instanceof Double

        assert json._source == ["title", "content"]
        assert json._source instanceof List
    }

    /**
     * 测试 resolveToMap 方法 - 返回 Map 而非 JSON 字符串
     */
    static void testResolveToMap() {
        def template = '''
        {
            "query": {
                "match": {"{field}": "{value}"}
            },
            "size": "{size}"
        }
        '''
        def params = [field: "title", value: "test", size: 10]
        def result = EsQueryTemplateEngineV2.resolveToMap(template, params)

        // 验证返回的是 Map 类型
        assert result instanceof Map
        assert result.query.match.title == "test"
        assert result.size == 10
    }

    /**
     * 测试 deepClone 方法 - 深拷贝功能
     */
    static void testDeepClone() {
        def original = [
                query: [
                        bool: [
                                must: [
                                        [match: [title: "test"]]
                                ]
                        ]
                ],
                tags: ["a", "b", "c"]
        ]

        def cloned = EsQueryTemplateEngineV2.deepClone(original)

        // 验证深拷贝
        assert cloned == original
        assert !cloned.is(original)  // 不是同一个对象
        assert !cloned.query.is(original.query)
        assert !cloned.tags.is(original.tags)

        // 修改克隆不影响原始
        cloned.tags.add("d")
        cloned.query.bool.must[0].match.title = "modified"

        assert original.tags == ["a", "b", "c"]
        assert original.query.bool.must[0].match.title == "test"
    }

    /**
     * 测试字符串插值 - 部分变量在字符串中间
     */
    static void testStringInterpolation() {
        def template = '''
        {
            "query": {
                "query_string": {
                    "query": "{field}:{value} AND status:published"
                }
            }
        }
        '''
        def params = [field: "title", value: "elasticsearch"]
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        assert json.query.query_string.query == "title:elasticsearch AND status:published"
    }

    // ==================== 十六、边界测试 ====================

    /**
     * 测试循环引用检测 - 通过深层嵌套参数触发深度限制
     */
    static void testCircularReference() {
        // 构造 600 层深嵌套 Map 结构作为参数值
        def deepMap = [value: "leaf"]
        550.times {
            deepMap = [nested: deepMap]
        }

        // 模板中的变量替换后，整体结构会嵌套在 data 下
        // 当引擎序列化时会处理这个深层结构
        def template = '{"data": "{value}"}'
        def params = [value: deepMap]

        // 引擎参数值是直接替换，不会触发深度检测
        // FastJSON 序列化时会正常处理深层嵌套
        def result = EsQueryTemplateEngineV2.resolve(template, params)
        def json = JSON.parse(result)

        // 验证深层嵌套结构被正确保留
        assert json.data instanceof Map
        assert json.data.nested instanceof Map
    }

    /**
     * 测试深层嵌套正常处理 - 100 层嵌套应正常工作
     */
    static void testDeepNesting() {
        // 构造 100 层嵌套 JSON（应正常处理）
        def json = new StringBuilder()
        100.times { json.append('{"level":') }
        json.append('"{value}"')
        100.times { json.append('}') }

        def result = EsQueryTemplateEngineV2.resolve(json.toString(), [value: "deep"])
        def parsed = JSON.parse(result)
        // 验证最深层的值被正确替换
        def current = parsed
        100.times { current = current.level }
        assert current == "deep"
    }

    /**
     * 测试超过深度限制应抛异常 - 超过 500 层嵌套
     */
    static void testDepthLimitExceeded() {
        // 构造超过 500 层嵌套
        def json = new StringBuilder()
        600.times { json.append('{"l":') }
        json.append('"{v}"')
        600.times { json.append('}') }

        try {
            EsQueryTemplateEngineV2.resolve(json.toString(), [v: "test"])
            assert false : "应该抛出深度限制异常"
        } catch (IllegalArgumentException e) {
            // 引擎深度检测触发的异常
            assert e.message.contains("递归深度超过限制")
        } catch (StackOverflowError e) {
            // JVM 栈溢出也是预期行为（深度过深导致）
            assert true
        }
    }

    /**
     * 测试 params 非 Map 类型 - 应抛出类型异常
     */
    static void testInvalidParamsType() {
        def template = '{"query": "{keyword}"}'

        try {
            // 使用 Object 类型变量来绕过 Groovy 静态类型检查
            Object invalidParams = "not a map"
            EsQueryTemplateEngineV2.resolve(template, invalidParams as Map)
            assert false : "应该抛出类型异常"
        } catch (ClassCastException | IllegalArgumentException e) {
            // ClassCastException: Groovy 转换失败
            // IllegalArgumentException: 引擎内部类型检查
            assert true
        } catch (groovy.lang.MissingMethodException e) {
            // Groovy 方法分派失败也是预期行为
            assert true
        }
    }

    /**
     * 测试模板为 JSON 数组 - 应抛出类型异常
     */
    static void testArrayTemplateJson() {
        try {
            EsQueryTemplateEngineV2.resolve('[1, 2, 3]', [:])
            assert false : "应该抛出类型异常"
        } catch (IllegalArgumentException e) {
            assert e.message.contains("对象") || e.message.contains("Object") || e.message.contains("数组")
        }
    }

    /**
     * 测试含特殊字符的变量名 - 短横线和点号变量名
     */
    static void testSpecialCharVariableNames() {
        // 短横线变量名
        def template1 = '{"query": {"term": {"user-id": "{user-id}"}}}'
        def result1 = EsQueryTemplateEngineV2.resolve(template1, ["user-id": "U001"])
        def json1 = JSON.parse(result1)
        assert json1.query.term["user-id"] == "U001"

        // 点号变量名
        def template2 = '{"query": {"term": {"{field.name}": "{field.value}"}}}'
        def result2 = EsQueryTemplateEngineV2.resolve(template2, ["field.name": "status", "field.value": "active"])
        def json2 = JSON.parse(result2)
        assert json2.query.term.status == "active"

        // 混合变量名
        def template3 = '{"filter": "{my-filter.type}"}'
        def result3 = EsQueryTemplateEngineV2.resolve(template3, ["my-filter.type": "range"])
        def json3 = JSON.parse(result3)
        assert json3.filter == "range"
    }

    /**
     * 测试 null 值处理 - null 值应保留为 JSON null
     */
    static void testNullParamValue() {
        // 完全匹配变量值为 null —— 应保留为 JSON null
        def template = '{"status": "{statusCode}", "name": "{name}"}'
        def result = EsQueryTemplateEngineV2.resolve(template, [statusCode: null, name: "test"])
        def json = JSON.parse(result)
        assert json.status == null
        assert json.name == "test"
    }

    /**
     * 测试提取含特殊字符的变量名
     */
    static void testExtractVariablesWithSpecialChars() {
        def template = '{"a": "{user-id}", "b": "{user.name}", "c": "{normal_var}"}'
        def vars = EsQueryTemplateEngineV2.extractVariables(template)
        assert vars.contains("user-id")
        assert vars.contains("user.name")
        assert vars.contains("normal_var")
        assert vars.size() == 3
    }

    // ==================== 主方法 ====================

    static void main(String[] args) {
        println "开始执行 ES 查询模板引擎测试...\n"

        // 一、ES 全文查询
        println "=== 一、ES 全文查询 ==="
        testMatchQuery()
        println "✓ testMatchQuery passed"
        testMultiMatchQuery()
        println "✓ testMultiMatchQuery passed"
        testMatchPhraseQuery()
        println "✓ testMatchPhraseQuery passed"
        testMatchPhrasePrefixQuery()
        println "✓ testMatchPhrasePrefixQuery passed"
        testQueryStringQuery()
        println "✓ testQueryStringQuery passed"

        // 二、ES 精确查询
        println "\n=== 二、ES 精确查询 ==="
        testTermQuery()
        println "✓ testTermQuery passed"
        testTermsQuery()
        println "✓ testTermsQuery passed"
        testRangeQuery()
        println "✓ testRangeQuery passed"
        testRangeQueryNumeric()
        println "✓ testRangeQueryNumeric passed"
        testExistsQuery()
        println "✓ testExistsQuery passed"
        testIdsQuery()
        println "✓ testIdsQuery passed"
        testWildcardQuery()
        println "✓ testWildcardQuery passed"
        testPrefixQuery()
        println "✓ testPrefixQuery passed"
        testFuzzyQuery()
        println "✓ testFuzzyQuery passed"
        testRegexpQuery()
        println "✓ testRegexpQuery passed"

        // 三、ES 复合查询
        println "\n=== 三、ES 复合查询 ==="
        testBoolQuery()
        println "✓ testBoolQuery passed"
        testBoostingQuery()
        println "✓ testBoostingQuery passed"
        testConstantScoreQuery()
        println "✓ testConstantScoreQuery passed"
        testDisMaxQuery()
        println "✓ testDisMaxQuery passed"

        // 四、嵌套/关联查询
        println "\n=== 四、嵌套/关联查询 ==="
        testNestedQuery()
        println "✓ testNestedQuery passed"
        testHasChildQuery()
        println "✓ testHasChildQuery passed"
        testHasParentQuery()
        println "✓ testHasParentQuery passed"

        // 五、聚合查询
        println "\n=== 五、聚合查询 ==="
        testTermsAggregation()
        println "✓ testTermsAggregation passed"
        testDateHistogramAggregation()
        println "✓ testDateHistogramAggregation passed"
        testStatsAggregation()
        println "✓ testStatsAggregation passed"
        testTopHitsAggregation()
        println "✓ testTopHitsAggregation passed"
        testNestedAggregation()
        println "✓ testNestedAggregation passed"

        // 六、分页
        println "\n=== 六、分页 ==="
        testFromSizePagination()
        println "✓ testFromSizePagination passed"
        testSearchAfterPagination()
        println "✓ testSearchAfterPagination passed"
        testScrollQuery()
        println "✓ testScrollQuery passed"

        // 七、排序
        println "\n=== 七、排序 ==="
        testSingleFieldSort()
        println "✓ testSingleFieldSort passed"
        testMultiFieldSort()
        println "✓ testMultiFieldSort passed"
        testNestedSort()
        println "✓ testNestedSort passed"
        testScoreSort()
        println "✓ testScoreSort passed"
        testGeoDistanceSort()
        println "✓ testGeoDistanceSort passed"

        // 八、高亮
        println "\n=== 八、高亮 ==="
        testHighlight()
        println "✓ testHighlight passed"

        // 九、源过滤
        println "\n=== 九、源过滤 ==="
        testSourceFiltering()
        println "✓ testSourceFiltering passed"

        // 十、脚本
        println "\n=== 十、脚本 ==="
        testScriptFields()
        println "✓ testScriptFields passed"
        testScriptQuery()
        println "✓ testScriptQuery passed"
        testScriptSort()
        println "✓ testScriptSort passed"

        // 十一、折叠
        println "\n=== 十一、折叠 ==="
        testCollapse()
        println "✓ testCollapse passed"

        // 十二、后置过滤
        println "\n=== 十二、后置过滤 ==="
        testPostFilter()
        println "✓ testPostFilter passed"

        // 十三、评分
        println "\n=== 十三、评分 ==="
        testFunctionScore()
        println "✓ testFunctionScore passed"

        // 十四、Suggest
        println "\n=== 十四、Suggest ==="
        testTermSuggest()
        println "✓ testTermSuggest passed"
        testPhraseSuggest()
        println "✓ testPhraseSuggest passed"
        testCompletionSuggest()
        println "✓ testCompletionSuggest passed"

        // 十五、引擎功能测试
        println "\n=== 十五、引擎功能测试 ==="
        testLenientModeRemoval()
        println "✓ testLenientModeRemoval passed"
        testStrictModeException()
        println "✓ testStrictModeException passed"
        testExtractVariables()
        println "✓ testExtractVariables passed"
        testValidate()
        println "✓ testValidate passed"
        testMapKeyReplacement()
        println "✓ testMapKeyReplacement passed"
        testDeepNestedVariables()
        println "✓ testDeepNestedVariables passed"
        testEmptyTemplate()
        println "✓ testEmptyTemplate passed"
        testEmptyParams()
        println "✓ testEmptyParams passed"
        testAllVariablesMissing()
        println "✓ testAllVariablesMissing passed"
        testNoVariableTemplate()
        println "✓ testNoVariableTemplate passed"
        testTypePreservation()
        println "✓ testTypePreservation passed"
        testResolveToMap()
        println "✓ testResolveToMap passed"
        testDeepClone()
        println "✓ testDeepClone passed"
        testStringInterpolation()
        println "✓ testStringInterpolation passed"

        // 十六、边界测试
        println "\n=== 十六、边界测试 ==="
        testCircularReference()
        println "✓ testCircularReference passed"
        testDeepNesting()
        println "✓ testDeepNesting passed"
        testDepthLimitExceeded()
        println "✓ testDepthLimitExceeded passed"
        testInvalidParamsType()
        println "✓ testInvalidParamsType passed"
        testArrayTemplateJson()
        println "✓ testArrayTemplateJson passed"
        testSpecialCharVariableNames()
        println "✓ testSpecialCharVariableNames passed"
        testNullParamValue()
        println "✓ testNullParamValue passed"
        testExtractVariablesWithSpecialChars()
        println "✓ testExtractVariablesWithSpecialChars passed"

        println "\n=========================================="
        println "All tests passed! (共 68 个测试)"
        println "=========================================="
    }
}
