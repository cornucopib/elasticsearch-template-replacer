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
                "scores": [100, "{score}", 300]
            }
        }'''
        def params = [age: 30, isActive: true, score: 200]

        when:
        def result = replacer.processQuery(dsl, params)

        then:
        JSON.parse(result) == JSON.parse('''{
            "term": {
                "age": 30,
                "active": true,
                "scores": [100, 200, 300]
            }
        }''')
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
}