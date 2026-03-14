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
        def params = [status1: "active"]

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
        def params = [username1: "john_doe"]

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
                "source": "doc['price'].value * {discount}",
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
        parsed.script.source == "doc['price'].value * 0.9"
        parsed.script.params.discount == 0.9
    }

    def "处理复杂脚本"() {
        given:
        def dsl = '''{
        "script": {
            "source": "def total = doc['price'].value * {multiplier}; if (params.apply_tax) { total *= {taxRate} }; return total;",
            "params": {
                "multiplier": "{multValue}",
                "apply_tax": "{applyTax}"
            }
        }
    }'''
        def params = [multValue: 1.1, taxRate: 1.08, applyTax: true]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        def parsed = JSON.parse(result)
        parsed.script.source == "def total = doc['price'].value * 1.1; if (params.apply_tax) { total *= 1.08 }; return total;"
        parsed.script.params.multiplier == 1.1
        parsed.script.params.apply_tax == true
    }

    def "处理脚本中的嵌套变量"() {
        given:
        def dsl = '''{
        "script": {
            "source": "return {base} + {bonus};",
            "params": {
                "base": "{baseValue}",
                "bonus": "{bonusValue}"
            }
        }
    }'''
        def params = [baseValue: 1000, bonusValue: 200]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        def parsed = JSON.parse(result)
        parsed.script.source == "return 1000 + 200;"
        parsed.script.params.base == 1000
        parsed.script.params.bonus == 200
    }

    def "处理脚本中的未解析变量"() {
        given:
        def dsl = '''{
        "script": {
            "source": "doc['price'].value * {discount}",
            "params": {
                "discount": "{discountValue}"
            }
        }
    }'''
        def params = [:] // 无参数

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        def parsed = JSON.parse(result)
        parsed.script.source == "doc['price'].value * {discount}"
        parsed.script.params.discount == "{discountValue}"
    }

    def "处理多维数组深度嵌套"() {
        given:
        // 使用更简单的JSON结构来确保格式正确
        def dsl = '''{
        "matrix": [
            [{"value": "{v11}"}, [1, "{v12}"]],
            ["{v21}", {"nested": "{v22}"}]
        ]
    }'''
        def params = [v11: "a", v12: "b", v21: "c", v22: "d"]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        def parsed = JSON.parse(result)
        // 不使用整体JSON对比，改为逐个字段断言，更精确
        parsed.matrix[0][0].value == "a"
        parsed.matrix[0][1] == [1, "b"]
        parsed.matrix[1][0] == "c"
        parsed.matrix[1][1].nested == "d"
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
}