import json
import uuid

import pytest

from tair import ScandocidResult, Tair


class TestTairSearch:
    def test_tft_createindex(self, t: Tair):
        index = "idx_" + str(uuid.uuid4())
        mappings = """
{
  "mappings": {
    "_source": { "enabled": true },
    "properties": {
      "product_id": { "type": "keyword", "ignore_above": 128 },
      "product_name": { "type": "text" },
      "product_title": { "type": "text", "analyzer": "jieba" },
      "price": { "type": "double" }
    }
  }
}"""

        assert t.tft_createindex(index, mappings)
        t.delete(index)

    def test_tft_updateindex(self, t: Tair):
        index = "idx_" + str(uuid.uuid4())
        mappings1 = """
{
  "mappings": {
    "_source": { "enabled": true },
    "properties": {
      "product_id": { "type": "keyword", "ignore_above": 128 },
      "product_name": { "type": "text" },
      "product_title": { "type": "text", "analyzer": "jieba" },
      "price": { "type": "double" }
    }
  }
}"""
        mappings2 = """
{
  "mappings": {
    "properties": { "product_group": { "type": "text", "analyzer": "chinese" } }
  }
}"""

        assert t.tft_createindex(index, mappings1)
        assert t.tft_updateindex(index, mappings2)
        t.delete(index)

    def test_tft_getindex(self, t: Tair):
        index = "idx_" + str(uuid.uuid4())
        mappings = """
{
  "mappings": {
    "_source": { "enabled": true },
    "properties": {
      "product_id": { "type": "keyword", "ignore_above": 128 },
      "product_name": { "type": "text" },
      "product_title": { "type": "text", "analyzer": "jieba" },
      "price": { "type": "double" }
    }
  }
}"""

        assert t.tft_getindex(index) is None
        assert t.tft_createindex(index, mappings)
        assert t.tft_getindex(index) is not None
        mappings1 = f"""
{{
  "{index}":{{
    "mappings":{{
      "_source":{{
        "enabled":true,
        "excludes":[
        ],
        "includes":[
        ]
      }},
      "dynamic":"false",
      "properties":{{
        "price":{{
          "boost":1.0,
          "enabled":true,
          "ignore_above":-1,
          "index":true,
          "similarity":"classic",
          "type":"double"
        }},
        "product_id":{{
          "boost":1.0,
          "enabled":true,
          "ignore_above":128,
          "index":true,
          "similarity":"classic",
          "type":"keyword"
        }},
        "product_name":{{
          "boost":1.0,
          "enabled":true,
          "ignore_above":-1,
          "index":true,
          "similarity":"classic",
          "type":"text"
        }},
        "product_title":{{
          "analyzer":"jieba",
          "boost":1.0,
          "enabled":true,
          "ignore_above":-1,
          "index":true,
          "similarity":"classic",
          "type":"text"
        }}
      }}
    }}
  }}
}}
"""
        result = t.tft_getindex_mappings(index)
        assert json.loads(mappings1) == json.loads(result)
        settings = f"""
{{
  "{index}":{{
    "settings":{{
    }}
  }}
}}
"""
        result = t.tft_getindex_settings(index)
        assert json.loads(settings) == json.loads(result)
        t.delete(index)

    def test_tft_adddoc(self, t: Tair):
        index = "idx_" + str(uuid.uuid4())
        mappings = """
{
  "mappings": {
    "_source": { "enabled": true },
    "properties": {
      "product_id": { "type": "keyword", "ignore_above": 128 },
      "product_name": { "type": "text" },
      "product_title": { "type": "text", "analyzer": "jieba" },
      "price": { "type": "double" }
    }
  }
}"""
        document = '{"product_id":"product test"}'

        assert t.tft_createindex(index, mappings)
        assert t.tft_adddoc(index, document, doc_id="00001") == '{"_id":"00001"}'
        assert t.tft_adddoc(index, document)
        t.delete(index)

    def test_tft_madddoc(self, t: Tair):
        index = "idx_" + str(uuid.uuid4())
        mappings = """
{
  "mappings": {
    "_source": { "enabled": true },
    "properties": {
      "product_id": { "type": "keyword", "ignore_above": 128 },
      "product_name": { "type": "text" },
      "product_title": { "type": "text", "analyzer": "jieba" },
      "price": { "type": "double" }
    }
  }
}"""
        document1 = '{"product_id":"test1"}'
        document2 = '{"product_id":"test2"}'

        assert t.tft_createindex(index, mappings)
        assert t.tft_madddoc(index, {document1: "00001", document2: "00002"})
        t.delete(index)

    def test_tft_updatedocfield(self, t: Tair):
        index = "idx_" + str(uuid.uuid4())
        mappings = """
{
  "mappings": {
    "_source": { "enabled": true },
    "properties": {
      "product_id": { "type": "keyword", "ignore_above": 128 },
      "product_name": { "type": "text" },
      "product_title": { "type": "text", "analyzer": "jieba" },
      "price": { "type": "double" }
    }
  }
}"""
        document1 = '{"product_id":"test1"}'
        document2 = '{"product_id":"test2"}'

        assert t.tft_createindex(index, mappings)
        assert t.tft_adddoc(index, document1, doc_id="00001") == '{"_id":"00001"}'
        assert t.tft_updatedocfield(index, "00001", document2)
        t.delete(index)

    def test_tft_deldocfield(self, t: Tair):
        index = "idx_" + str(uuid.uuid4())
        mappings = """
{
  "mappings": {
    "_source": { "enabled": true },
    "properties": {
      "product_id": { "type": "keyword", "ignore_above": 128 },
      "product_name": { "type": "text" },
      "product_title": { "type": "text", "analyzer": "jieba" },
      "price": { "type": "double" }
    }
  }
}"""
        document = '{"product_id":"test1","product_name":"product test"}'

        assert t.tft_createindex(index, mappings)
        assert t.tft_adddoc(index, document, doc_id="00001") == '{"_id":"00001"}'
        assert t.tft_deldocfield(index, "00001", ["product_name"]) == 1
        t.delete(index)

    def test_tft_incrlongdocfield(self, t: Tair):
        index = "idx_" + str(uuid.uuid4())
        mappings = """
{
  "mappings": {
    "_source": { "enabled": true },
    "properties": {
      "product_id": { "type": "keyword", "ignore_above": 128 },
      "product_name": { "type": "text" },
      "product_title": { "type": "text", "analyzer": "jieba" },
      "price": { "type": "integer" }
    }
  }
}"""
        document = '{"product_id":"test1","price":100}'

        assert t.tft_createindex(index, mappings)
        assert t.tft_adddoc(index, document, doc_id="00001") == '{"_id":"00001"}'
        assert t.tft_incrlongdocfield(index, "00001", "price", 200) == 300
        t.delete(index)

    def test_tft_incrfloatdocfield(self, t: Tair):
        index = "idx_" + str(uuid.uuid4())
        mappings = """
{
  "mappings": {
    "_source": { "enabled": true },
    "properties": {
      "product_id": { "type": "keyword", "ignore_above": 128 },
      "product_name": { "type": "text" },
      "product_title": { "type": "text", "analyzer": "jieba" },
      "price": { "type": "double" }
    }
  }
}"""
        document = '{"product_id":"test1","price":1.1}'

        assert t.tft_createindex(index, mappings)
        assert t.tft_adddoc(index, document, doc_id="00001") == '{"_id":"00001"}'
        assert t.tft_incrfloatdocfield(index, "00001", "price", 2.2) == pytest.approx(
            3.3
        )
        t.delete(index)

    def test_tft_getdoc(self, t: Tair):
        index = "idx_" + str(uuid.uuid4())
        mappings = """
{
  "mappings": {
    "_source": { "enabled": true },
    "properties": {
      "product_id": { "type": "keyword", "ignore_above": 128 },
      "product_name": { "type": "text" },
      "product_title": { "type": "text", "analyzer": "jieba" },
      "price": { "type": "double" }
    }
  }
}"""
        document = '{"product_id":"test1","price":1.1}'

        assert t.tft_createindex(index, mappings)
        assert t.tft_adddoc(index, document, doc_id="00001") == '{"_id":"00001"}'
        assert t.tft_getdoc(index, "00002") is None
        assert (
            t.tft_getdoc(index, "00001")
            == '{"_id":"00001","_source":{"product_id":"test1","price":1.1}}'
        )
        t.delete(index)

    def test_tft_exists(self, t: Tair):
        index = "idx_" + str(uuid.uuid4())
        mappings = """
{
  "mappings": {
    "_source": { "enabled": true },
    "properties": {
      "product_id": { "type": "keyword", "ignore_above": 128 },
      "product_name": { "type": "text" },
      "product_title": { "type": "text", "analyzer": "jieba" },
      "price": { "type": "double" }
    }
  }
}"""
        document = '{"product_id":"test1","price":1.1}'

        assert t.tft_createindex(index, mappings)
        assert t.tft_adddoc(index, document, doc_id="00001") == '{"_id":"00001"}'
        assert t.tft_exists(index, "00001") == 1
        assert t.tft_exists(index, "00002") == 0
        t.delete(index)

    def test_tft_docnum(self, t: Tair):
        index = "idx_" + str(uuid.uuid4())
        mappings = """
{
  "mappings": {
    "_source": { "enabled": true },
    "properties": {
      "product_id": { "type": "keyword", "ignore_above": 128 },
      "product_name": { "type": "text" },
      "product_title": { "type": "text", "analyzer": "jieba" },
      "price": { "type": "double" }
    }
  }
}"""
        document1 = '{"product_id":"test1"}'
        document2 = '{"product_id":"test2"}'

        assert t.tft_createindex(index, mappings)
        assert t.tft_madddoc(index, {document1: "00001", document2: "00002"})
        assert t.tft_docnum(index) == 2
        t.delete(index)

    def test_tft_scandocid(self, t: Tair):
        index = "idx_" + str(uuid.uuid4())
        mappings = """
{
  "mappings": {
    "_source": { "enabled": true },
    "properties": {
      "product_id": { "type": "keyword", "ignore_above": 128 },
      "product_name": { "type": "text" },
      "product_title": { "type": "text", "analyzer": "jieba" },
      "price": { "type": "double" }
    }
  }
}"""
        document1 = '{"product_id":"test1"}'
        document2 = '{"product_id":"test2"}'
        document3 = '{"product_id":"test3"}'

        assert t.tft_createindex(index, mappings)
        assert t.tft_madddoc(
            index, {document1: "00001", document2: "00002", document3: "00003"}
        )
        assert t.tft_scandocid(index, 0, count=3, match="*") == ScandocidResult(
            "0", ["00001", "00002", "00003"]
        )
        t.delete(index)

    def test_tft_deldoc(self, t: Tair):
        index = "idx_" + str(uuid.uuid4())
        mappings = """
{
  "mappings": {
    "_source": { "enabled": true },
    "properties": {
      "product_id": { "type": "keyword", "ignore_above": 128 },
      "product_name": { "type": "text" },
      "product_title": { "type": "text", "analyzer": "jieba" },
      "price": { "type": "double" }
    }
  }
}"""
        document = '{"product_id":"test1","product_name":"product test"}'

        assert t.tft_createindex(index, mappings)
        assert t.tft_adddoc(index, document, doc_id="00001") == '{"_id":"00001"}'
        assert t.tft_deldoc(index, {"00001", "00002"}) == 1
        t.delete(index)

    def test_tft_delall(self, t: Tair):
        index = "idx_" + str(uuid.uuid4())
        mappings = """
{
  "mappings": {
    "_source": { "enabled": true },
    "properties": {
      "product_id": { "type": "keyword", "ignore_above": 128 },
      "product_name": { "type": "text" },
      "product_title": { "type": "text", "analyzer": "jieba" },
      "price": { "type": "double" }
    }
  }
}"""
        document = '{"product_id":"test1","product_name":"product test"}'

        assert t.tft_createindex(index, mappings)
        assert t.tft_adddoc(index, document, doc_id="00001") == '{"_id":"00001"}'
        assert t.tft_delall(index)
        t.delete(index)

    def test_tft_search(self, t: Tair):
        index = "idx_" + str(uuid.uuid4())
        mappings = """
{
  "mappings": {
    "_source": { "enabled": true },
    "properties": {
      "product_id": { "type": "keyword", "ignore_above": 128 },
      "product_name": { "type": "text" },
      "product_title": { "type": "text", "analyzer": "jieba" },
      "price": { "type": "double" }
    }
  }
}"""
        document1 = '{"product_id":"test1"}'
        document2 = '{"product_id":"test2"}'
        document3 = '{"product_id":"test3"}'

        assert t.tft_createindex(index, mappings)
        assert t.tft_madddoc(
            index, {document1: "00001", document2: "00002", document3: "00003"}
        )

        want = f"""{{
  "hits": {{
    "hits": [
      {{
        "_id": "00001",
        "_index": "{index}",
        "_score": 1.0,
        "_source": {{ "product_id": "test1" }}
      }},
      {{
        "_id": "00002",
        "_index": "{index}",
        "_score": 1.0,
        "_source": {{ "product_id": "test2" }}
      }},
      {{
        "_id": "00003",
        "_index": "{index}",
        "_score": 1.0,
        "_source": {{ "product_id": "test3" }}
      }}
    ],
    "max_score": 1.0,
    "total": {{ "relation": "eq", "value": 3 }}
  }}
}}"""
        result = t.tft_search(index, '{"sort":[{"price":{"order":"desc"}}]}')
        assert json.loads(want) == json.loads(result)
        result = t.tft_search(index, '{"sort":[{"price":{"order":"desc"}}]}', True)
        assert json.loads(want) == json.loads(result)
        result = t.tft_explaincost(index, '{"sort":[{"price":{"order":"desc"}}]}')
        assert json.loads(result)["QUERY_COST"]
        t.delete(index)

    def test_tft_msearch(self, t: Tair):
        index1 = "{idx}_" + str(uuid.uuid4())
        index2 = "{idx}_" + str(uuid.uuid4())
        mappings = """
{
  "mappings": {
    "_source": { "enabled": true },
    "properties": {
      "product_id": { "type": "keyword", "ignore_above": 128 },
      "product_name": { "type": "text" },
      "product_title": { "type": "text", "analyzer": "jieba" },
      "price": { "type": "double" }
    }
  }
}"""
        document1 = '{"product_id":"test1"}'
        document2 = '{"product_id":"test2"}'
        document3 = '{"product_id":"test3"}'
        document4 = '{"product_id":"test4"}'

        assert t.tft_createindex(index1, mappings)
        assert t.tft_createindex(index2, mappings)
        assert t.tft_madddoc(index1, {document1: "00001", document2: "00002"})
        assert t.tft_madddoc(index2, {document3: "00003", document4: "00004"})

        want = f"""{{
    "aux_info": {{"index_crc64": 5843875291690071373}},
    "hits": {{
        "hits": [
          {{
            "_id": "00001",
            "_index": "{index1}",
            "_score": 1.0,
            "_source": {{ "product_id": "test1" }}
          }},
          {{
            "_id": "00002",
            "_index": "{index1}",
            "_score": 1.0,
            "_source": {{ "product_id": "test2" }}
          }},
          {{
            "_id": "00003",
            "_index": "{index2}",
            "_score": 1.0,
            "_source": {{ "product_id": "test3" }}
          }},
          {{
            "_id": "00004",
            "_index": "{index2}",
            "_score": 1.0,
            "_source": {{ "product_id": "test4" }}
          }}
        ],
        "max_score": 1.0,
        "total": {{ "relation": "eq", "value": 4 }}
      }}
    }}"""
        result = t.tft_msearch(
            2, {index1, index2}, '{"sort":[{"_doc":{"order":"asc"}}]}'
        )
        assert json.loads(want) == json.loads(result)
        t.delete(index1)
        t.delete(index2)

    def test_tft_analyzer(self, t: Tair):
        index = "idx_" + str(uuid.uuid4())
        mappings = """
{
  "mappings":{
    "properties":{
      "f0":{
        "type":"text",
        "analyzer":"my_analyzer"
      }
    }
  },
  "settings":{
    "analysis":{
      "analyzer":{
        "my_analyzer":{
          "type":"standard"
        }
      }
    }
  }
}"""
        text = "This is tair-py."

        assert t.tft_createindex(index, mappings)
        assert t.tft_analyzer("standard", text) == t.tft_analyzer(
            "my_analyzer", text, index
        )
        assert "consuming time" in str(t.tft_analyzer("standard", text, None, True))

        t.delete(index)

    def test_tft_addsug(self, t: Tair):
        index = "idx_" + str(uuid.uuid4())

        assert (
            t.tft_addsug(index, {"redis is a memory database": 3, "redis cluster": 10})
            == 2
        )
        t.delete(index)

    def test_tft_delsug(self, t: Tair):
        index = "idx_" + str(uuid.uuid4())

        assert (
            t.tft_addsug(index, {"redis is a memory database": 3, "redis cluster": 10})
            == 2
        )
        assert t.tft_delsug(index, ("redis is a memory database", "redis cluster")) == 2
        t.delete(index)

    def test_tft_sugnum(self, t: Tair):
        index = "idx_" + str(uuid.uuid4())

        assert (
            t.tft_addsug(index, {"redis is a memory database": 3, "redis cluster": 10})
            == 2
        )
        assert t.tft_sugnum(index) == 2
        t.delete(index)

    def test_tft_getsug(self, t: Tair):
        index = "idx_" + str(uuid.uuid4())

        assert (
            t.tft_addsug(index, {"redis is a memory database": 3, "redis cluster": 10})
            == 2
        )
        assert sorted(t.tft_getsug(index, "res", max_count=2, fuzzy=True)) == [
            "redis cluster",
            "redis is a memory database",
        ]
        t.delete(index)

    def test_tft_getallsugs(self, t: Tair):
        index = "idx_" + str(uuid.uuid4())

        assert (
            t.tft_addsug(index, {"redis is a memory database": 3, "redis cluster": 10})
            == 2
        )
        assert sorted(t.tft_getallsugs(index)) == [
            "redis cluster",
            "redis is a memory database",
        ]
        t.delete(index)

    def test_scandocid_result_eq(self):
        assert ScandocidResult("0", ["00001", "00002", "00003"]) == ScandocidResult(
            "0", ["00001", "00002", "00003"]
        )
        assert not ScandocidResult("0", ["00001", "00002", "00003"]) == ScandocidResult(
            "0", ["00001", "00002"]
        )
        assert not ScandocidResult("0", ["00001", "00002", "00003"]) == 1

    def test_scandocid_result_ne(self):
        assert not ScandocidResult("0", ["00001", "00002", "00003"]) != ScandocidResult(
            "0", ["00001", "00002", "00003"]
        )
        assert ScandocidResult("0", ["00001", "00002", "00003"]) != ScandocidResult(
            "0", ["00001", "00002"]
        )
        assert ScandocidResult("0", ["00001", "00002", "00003"]) != 1

    def test_scandocid_result_repr(self):
        assert (
            str(ScandocidResult("10", ["00001", "00002", "00003"]))
            == f"{{cursor: 10, doc_ids: ['00001', '00002', '00003']}}"
        )
