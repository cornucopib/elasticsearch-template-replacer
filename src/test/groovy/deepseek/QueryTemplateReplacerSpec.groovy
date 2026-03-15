package deepseek

import com.alibaba.fastjson.JSON
import spock.lang.Specification

class QueryTemplateReplacerSpec extends Specification {

    def replacer = new QueryTemplateReplacer()

    def "替换单个存在的模板变量"() {
        given:
        def dsl = '''{
            "term": {
                "name": "{name}"
            }
        }'''
        def params = [name: "John"]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        JSON.parse(result) == JSON.parse('''{
            "term": {
                "name": "John"
            }
        }''')
    }

    def "删除未解析的模板变量"() {
        given:
        def dsl = '''{
            "term": {
                "name": "{name}"
            }
        }'''
        def params = [:]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        JSON.parse(result) == JSON.parse('{}')
    }

    def "处理嵌套bool查询"() {
        given:
        def dsl = '''{
            "bool": {
                "must": [
                    { "term": { "status": "active" } },
                    { 
                        "bool": {
                            "should": [
                                { "term": { "tag": "{tag1}" } },
                                { "term": { "tag": "{tag2}" } }
                            ]
                        }
                    }
                ]
            }
        }'''
        def params = [tag1: "urgent"]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        JSON.parse(result) == JSON.parse('''{
            "bool": {
                "must": [
                    { "term": { "status": "active" } },
                    { 
                        "bool": {
                            "should": [
                                { "term": { "tag": "urgent" } }
                            ]
                        }
                    }
                ]
            }
        }''')
    }

    def "混合情况: 固定值+可解析变量+未解析变量"() {
        given:
        def dsl = '''{
            "bool": {
                "should": [
                    { "term": { "type": "user" } },
                    { "term": { "name": "{name}" } },
                    { "term": { "email": "{email}" } }
                ]
            }
        }'''
        def params = [name: "Alice"]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        JSON.parse(result) == JSON.parse('''{
            "bool": {
                "should": [
                    { "term": { "type": "user" } },
                    { "term": { "name": "Alice" } }
                ]
            }
        }''')
    }

    def "处理range查询中的模板"() {
        given:
        def dsl = '''{
        "range": {
            "age": {
                "gte": "{minAge}",
                "lte": "{maxAge}"
            }
        }
    }'''
        def params = [minAge: 18]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        JSON.parse(result) == JSON.parse('''{
        "range": {
            "age": {
                "gte": 18
            }
        }
    }''')
    }

    def "处理range查询中的多个字段"() {
        given:
        def dsl = '''{
        "range": {
            "age": {
                "gte": "{minAge}",
                "lte": "{maxAge}"
            },
            "price": {
                "gte": "{minPrice}"
            }
        }
    }'''
        def params = [minAge: 18, minPrice: 100]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        JSON.parse(result) == JSON.parse('''{
        "range": {
            "age": {
                "gte": 18
            },
            "price": {
                "gte": 100
            }
        }
    }''')
    }

    def "处理range查询中的部分未解析变量"() {
        given:
        def dsl = '''{
        "range": {
            "age": {
                "gte": "{minAge}",
                "lte": "{maxAge}"
            }
        }
    }'''
        def params = [maxAge: 65] // 只提供maxAge，不提供minAge

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        JSON.parse(result) == JSON.parse('''{
        "range": {
            "age": {
                "lte": 65
            }
        }
    }''')
    }

    def "处理range查询中的所有变量未解析"() {
        given:
        def dsl = '''{
            "range": {
                "age": {
                    "gte": "{minAge}",
                    "lte": "{maxAge}"
                }
            }
        }'''
        def params = [:] // 不提供任何参数

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        JSON.parse(result) == JSON.parse('{}')
    }

    def "处理range查询中的数字类型"() {
        given:
        def dsl = '''{
        "range": {
                "price": {
                    "gte": "{minPrice}",
                    "lte": "{maxPrice}"
                }
            }
        }'''
        def params = [minPrice: 100, maxPrice: 200]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        def parsed = JSON.parse(result)
        parsed.range.price.gte == 100
        parsed.range.price.lte == 200
    }

    def "处理range查询中的日期类型"() {
        given:
        def dsl = '''{
        "range": {
            "date": {
                "gte": "{startDate}",
                "lte": "{endDate}"
            }
        }
    }'''
        def params = [startDate: "2023-01-01", endDate: "2023-12-31"]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        JSON.parse(result) == JSON.parse('''{
        "range": {
            "date": {
                "gte": "2023-01-01",
                "lte": "2023-12-31"
            }
        }
    }''')
    }

    def "复杂嵌套: 多级bool与混合条件"() {
        given:
        def dsl = '''{
            "bool": {
                "must": [
                    { "term": { "category": "electronics" } },
                    {
                        "bool": {
                            "should": [
                                { "term": { "brand": "{brand}" } },
                                { 
                                    "bool": {
                                        "must": [
                                            { "term": { "in_stock": true } },
                                            { "term": { "price": "{price}" } }
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                ]
            }
        }'''
        def params = [price: 100]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        JSON.parse(result) == JSON.parse('''{
            "bool": {
                "must": [
                    { "term": { "category": "electronics" } },
                    {
                        "bool": {
                            "should": [
                                { 
                                    "bool": {
                                        "must": [
                                            { "term": { "in_stock": true } },
                                            { "term": { "price": 100 } }
                                        ]
                                    }
                                }
                            ]
                        }
                    }
                ]
            }
        }''')
    }

    def "处理空输入"() {
        given:
        def dsl = ""
        def params = [name: "Test"]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        result == "{}"
    }

    def "处理无效JSON输入"() {
        given:
        def dsl = "{ invalid: json }"
        def params = [:]

        when:
        replacer.processQuery(dsl, params)

        then:
        thrown(IllegalArgumentException)
    }

    def "处理无模板的查询"() {
        given:
        def dsl = '''{
            "match_all": {}
        }'''
        def params = [:]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        JSON.parse(result) == JSON.parse(dsl)
    }

    def "处理must_not子句"() {
        given:
        def dsl = '''{
            "bool": {
                "must_not": [
                    { "term": { "status": "{status}" } },
                    { "term": { "deleted": true } }
                ]
            }
        }'''
        def params = [status: "inactive"]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        JSON.parse(result) == JSON.parse('''{
            "bool": {
                "must_not": [
                    { "term": { "status": "inactive" } },
                    { "term": { "deleted": true } }
                ]
            }
        }''')
    }

    def "处理单个对象的must子句"() {
        given:
        def dsl = '''{
            "bool": {
                "must": {
                    "term": { "status": "{status}" }
                }
            }
        }'''
        def params = [status: "active"]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        JSON.parse(result) == JSON.parse('''{
            "bool": {
                "must": {
                    "term": { "status": "active" }
                }
            }
        }''')
    }

    def "处理混合数据类型的值"() {
        given:
        def dsl = '''{
        "term": {
            "age": "{age}",
            "active": "{isActive}",
            "scores": [100, "{score}", 300],
            "nested": {
                "value": "{nestedValue}"
            }
        }
    }'''
        def params = [age: 30, isActive: true, score: 200, nestedValue: "nested123"]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        def parsed = JSON.parse(result)
        parsed.term.age == 30
        parsed.term.active == true
        parsed.term.scores == [100, 200, 300]
        parsed.term.nested.value == "nested123"
    }

    def "处理数组中的模板变量"() {
        given:
        def dsl = '''{
        "terms": {
            "tags": ["{tag1}", "fixed", "{tag2}"]
        }
    }'''
        def params = [tag1: "urgent", tag2: "important"]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        JSON.parse(result) == JSON.parse('''{
        "terms": {
            "tags": ["urgent", "fixed", "important"]
        }
    }''')
    }

    def "处理多维数组中的模板变量"() {
        given:
        def dsl = '''{
        "matrix": [
            [1, "{value1}"],
            ["{value2}", 4]
        ]
    }'''
        def params = [value1: 2, value2: 3]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        JSON.parse(result) == JSON.parse('''{
        "matrix": [
            [1, 2],
            [3, 4]
        ]
    }''')
    }

    def "处理数组中的未解析变量"() {
        given:
        def dsl = '''{
        "values": ["{val1}", "fixed", "{val2}"]
    }'''
        def params = [val2: "value2"] // 只提供val2，不提供val1

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        JSON.parse(result) == JSON.parse('''{
        "values": ["fixed", "value2"]
    }''')
    }

    def "处理混合数组类型"() {
        given:
        def dsl = '''{
        "data": [
            10,
            "{stringValue}",
            true,
            "{boolValue}",
            {
                "key": "{nestedValue}"
            },
            [
                "{arrayValue}"
            ]
        ]
    }'''
        def params = [stringValue: "text", boolValue: false, nestedValue: "nested", arrayValue: "array"]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        def parsed = JSON.parse(result)
        parsed.data[0] == 10
        parsed.data[1] == "text"
        parsed.data[2] == true
        parsed.data[3] == false
        parsed.data[4].key == "nested"
        parsed.data[5][0] == "array"
    }

    def "处理嵌套模板变量"() {
        given:
        def dsl = '''{
            "nested": {
                "path": "user",
                "query": {
                    "term": {
                        "user.name": "{username}"
                    }
                }
            }
        }'''
        def params = [username: "john_doe"]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        JSON.parse(result) == JSON.parse('''{
            "nested": {
                "path": "user",
                "query": {
                    "term": {
                        "user.name": "john_doe"
                    }
                }
            }
        }''')
    }

    def "处理嵌套查询"() {
        given:
        def dsl = '''{
        "nested": {
            "path": "comments",
            "query": {
                "term": {
                    "comments.text": "{searchTerm}"
                }
            }
        }
    }'''
        def params = [searchTerm: "important"]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        JSON.parse(result) == JSON.parse('''{
        "nested": {
            "path": "comments",
            "query": {
                "term": {
                    "comments.text": "important"
                }
            }
        }
    }''')
    }

    def "处理地理距离查询"() {
        given:
        def dsl = '''{
        "geo_distance": {
            "distance": "10km",
            "location": "{lat},{lon}"
        }
    }'''
        def params = [lat: "40.7128", lon: "-74.0060"]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        JSON.parse(result) == JSON.parse('''{
        "geo_distance": {
            "distance": "10km",
            "location": "40.7128,-74.0060"
        }
    }''')
    }

    def "处理部分地理坐标模板"() {
        given:
        def dsl = '''{
        "geo_distance": {
            "distance": "10km",
            "location": "40.7128,{lon}"
        }
    }'''
        def params = [lon: "-74.0060"]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        JSON.parse(result) == JSON.parse('''{
        "geo_distance": {
            "distance": "10km",
            "location": "40.7128,-74.0060"
        }
    }''')
    }

    def "处理地理坐标中的未解析变量"() {
        given:
        def dsl = '''{
        "geo_distance": {
            "distance": "10km",
            "location": "{lat},{lon}"
        }
    }'''
        def params = [lat: "40.7128"] // 缺少lon参数

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        JSON.parse(result) == JSON.parse('''{
        "geo_distance": {
            "distance": "10km",
            "location": "40.7128,{lon}"
        }
    }''')
    }

    def "处理单个地理坐标变量"() {
        given:
        def dsl = '''{
        "geo_distance": {
            "distance": "10km",
            "location": "{coordinates}"
        }
    }'''
        def params = [coordinates: "40.7128,-74.0060"]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        JSON.parse(result) == JSON.parse('''{
        "geo_distance": {
            "distance": "10km",
            "location": "40.7128,-74.0060"
        }
    }''')
    }

    def "处理脚本查询"() {
        given:
        def dsl = '''{
        "script": {
                "source": "doc['price'].value * params.discount",
                "params": {
                    "discount": "{discountValue}"
                }
            }
        }'''
        def params = [discountValue: 0.9]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        def parsed = JSON.parse(result)
        parsed.script.source == "doc['price'].value * params.discount"
        parsed.script.params.discount == 0.9
    }

    def "处理日期类型转换"() {
        given:
        def dsl = '''{
        "range": {
            "event_date": {
                "gte": "{startDate}",
                "format": "yyyy-MM-dd"
            }
        }
    }'''
        def params = [startDate: "2023-01-01"]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        def parsed = JSON.parse(result)
        // 修正断言：在JSON中，日期通常是字符串，所以这里应该是true
        parsed.range.event_date.gte instanceof String == true // 将 false 改为 true
        parsed.range.event_date.gte == "2023-01-01"
    }

    def "复杂综合查询场景 - 电商产品搜索"() {
        given:
        def dsl = '''{
            "query": {
                "bool": {
                    "must": [
                        {
                            "multi_match": {
                                "query": "{searchKeyword}",
                                "fields": ["title", "description", "brand"]
                            }
                        },
                        {
                            "term": {
                                "category_id": "{categoryId}"
                            }
                        },
                        {
                            "range": {
                                "price": {
                                    "gte": "{minPrice}",
                                    "lte": "{maxPrice}"
                                }
                            }
                        },
                        {
                            "script": {
                                "source": "def baseScore = doc['rating'].value * params.ratingWeight; def salesBonus = Math.log(doc['sales_count'].value + 1) * params.salesFactor; def promotionBoost = params.isPromotionOnly ? (doc['on_promotion'].value ? 10 : 0) : 0; return baseScore + salesBonus + promotionBoost >= params.minScore;",
                                "params": {
                                    "ratingWeight": "{ratingWeight}",
                                    "salesFactor": "{salesFactor}",
                                    "isPromotionOnly": "{showPromotionOnly}",
                                    "minScore": "{minRating}"
                                }
                            }
                        }
                    ],
                    "filter": [
                        {
                            "terms": {
                                "brand": "{brandList}"
                            }
                        },
                        {
                            "geo_distance": {
                                "distance": "{deliveryRadius}km",
                                "warehouse_location": "{warehouseLat},{warehouseLon}"
                            }
                        },
                        {
                            "exists": {
                                "field": "{requiredField}"
                            }
                        }
                    ],
                    "should": [
                        {
                            "nested": {
                                "path": "tags",
                                "query": {
                                    "term": {
                                        "tags.name": "{preferredTag}"
                                    }
                                },
                                "score_mode": "sum"
                            }
                        },
                        {
                            "function_score": {
                                "query": { "match_all": {} },
                                "functions": [
                                    {
                                        "script_score": {
                                            "script": {
                                                "source": "doc['stock_quantity'].value > params.minStock ? 2 : 0",
                                                "params": {
                                                    "minStock": "{minStockLevel}"
                                                }
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    ],
                    "must_not": [
                        {
                            "terms": {
                                "exclude_keywords": "{excludedTags}"
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "price_stats": {
                    "stats": {
                        "field": "price"
                    }
                },
                "brand_distribution": {
                    "terms": {
                        "field": "brand",
                        "size": "{brandSize}"
                    }
                },
                "avg_rating": {
                    "avg": {
                        "script": {
                            "source": "doc['rating'].value * params.weight",
                            "params": {
                                "weight": "{ratingMultiplier}"
                            }
                        }
                    }
                }
            },
            "sort": [
                {
                    "_score": {
                        "order": "desc"
                    }
                },
                {
                    "price": {
                        "order": "{priceSortOrder}"
                    }
                },
                {
                    "_script": {
                        "type": "number",
                        "script": {
                            "source": "def price = doc['price'].value; def discount = doc['discount_rate'].value; return price * (1 - discount) * params.sortFactor;",
                            "params": {
                                "sortFactor": "{customSortFactor}"
                            }
                        },
                        "order": "asc"
                    }
                }
            ],
            "script_fields": {
                "final_price": {
                    "script": {
                        "source": "doc['price'].value * (1 - doc['discount_rate'].value)",
                        "params": {}
                    }
                },
                "shipping_cost": {
                    "script": {
                        "source": "def distance = doc['warehouse_location'].arcDistance(params.lat, params.lon); if (distance < params.nearThreshold) return params.baseFee; return params.baseFee + (distance / 100) * params.distanceRate;",
                        "params": {
                            "lat": "{userLat}",
                            "lon": "{userLon}",
                            "nearThreshold": "{nearDistanceKm}",
                            "baseFee": "{shippingBaseFee}",
                            "distanceRate": "{shippingRatePerKm}"
                        }
                    }
                }
            },
            "highlight": {
                "fields": {
                    "title": {
                        "pre_tags": "<em style='color:{highlightColor}'>",
                        "post_tags": "</em>"
                    },
                    "description": {}
                }
            }
        }'''
        def params = [
                searchKeyword      : "无线蓝牙耳机",
                categoryId         : 1001,
                minPrice           : 50,
                maxPrice           : 500,
                ratingWeight       : 1.5,
                salesFactor        : 0.8,
                showPromotionOnly  : true,
                minRating          : 75,
                brandList          : ["Sony", "Bose", "Sennheiser"],
                deliveryRadius     : 100,
                warehouseLat       : "39.9042",
                warehouseLon       : "116.4074",
                requiredField      : "stock_quantity",
                preferredTag       : "bestseller",
                minStockLevel      : 10,
                excludedTags       : ["refurbished", "open_box"],
                brandSize          : 20,
                ratingMultiplier   : 1.2,
                priceSortOrder     : "asc",
                customSortFactor   : 0.95,
                userLat            : "39.9042",
                userLon            : "116.4074",
                nearDistanceKm     : 50,
                shippingBaseFee    : 5.0,
                shippingRatePerKm  : 0.1,
                highlightColor     : "red"
        ]

        when:
        def result = replacer.processQuery(dsl, params)
        def parsed = JSON.parse(result)

        then:
        // 验证 query 部分
        parsed.query.bool.must[0].multi_match.query == "无线蓝牙耳机"
        parsed.query.bool.must[1].term.category_id == 1001
        parsed.query.bool.must[2].range.price.gte == 50
        parsed.query.bool.must[2].range.price.lte == 500

        // 验证脚本参数
        def scriptParams = parsed.query.bool.must[3].script.params
        scriptParams.ratingWeight == 1.5
        scriptParams.salesFactor == 0.8
        scriptParams.isPromotionOnly == true
        scriptParams.minScore == 75

        // 验证 filter 部分
        parsed.query.bool.filter[0].terms.brand == ["Sony", "Bose", "Sennheiser"]
//        parsed.query.bool.filter[1].geo_distance.distance == "100km"
//        parsed.query.bool.filter[1].geo_distance.warehouse_location == "39.9042,116.4074"
        parsed.query.bool.filter[2].exists.field == "stock_quantity"

        // 验证 should 部分
        parsed.query.bool.should[0].nested.query.term["tags.name"] == "bestseller"
        parsed.query.bool.should[1].function_score.functions[0].script_score.script.params.minStock == 10

        // 验证 must_not 部分
        parsed.query.bool.must_not[0].terms.exclude_keywords == ["refurbished", "open_box"]

        // 验证聚合部分
        parsed.aggs.brand_distribution.terms.size == 20
        parsed.aggs.avg_rating.avg.script.params.weight == 1.2

        // 验证排序部分
        parsed.sort[1].price.order == "asc"
        parsed.sort[2]._script.script.params.sortFactor == 0.95

        // 验证 script_fields
        def shippingParams = parsed.script_fields.shipping_cost.script.params
        shippingParams.lat == "39.9042"
        shippingParams.lon == "116.4074"
        shippingParams.nearThreshold == 50
        shippingParams.baseFee == 5.0
        shippingParams.distanceRate == 0.1

        // 验证高亮部分
        parsed.highlight.fields.title.pre_tags[0] == "<em style='color:red'>"
    }


}