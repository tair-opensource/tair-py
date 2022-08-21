import uuid

from tair import Tair


class TestTairDoc:
    def test_json_set(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert t.json_set(
            key,
            ".",
            """{
    "store": {
        "book": [
            {
                "category": "reference",
                "author": "Nigel Rees",
                "title": "Sayings of the Century",
                "price": 8.95
            },
            {
                "category": "fiction",
                "author": "Evelyn Waugh",
                "title": "Sword of Honour",
                "price": 12.99
            },
            {
                "category": "fiction",
                "author": "Herman Melville",
                "title": "Moby Dick",
                "isbn": "0-553-21311-3",
                "price": 8.99
            },
            {
                "category": "fiction",
                "author": "J. R. R. Tolkien",
                "title": "The Lord of the Rings",
                "isbn": "0-395-19395-8",
                "price": 22.99
            }
        ],
        "bicycle": {
            "color": "red",
            "price": 19.95
        }
    }
}""",
        )

    def test_json_set_nx(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert t.json_set(
            key,
            ".",
            '{"foo": "bar", "baz" : 42}',
            nx=True,
        )

        assert (
            t.json_set(
                key,
                ".",
                '{"foo": "bar", "baz" : 42}',
                nx=True,
            )
            is None
        )

    def test_json_set_xx(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert (
            t.json_set(
                key,
                ".",
                '{"foo": "bar", "baz" : 42}',
                xx=True,
            )
            is None
        )

        assert t.json_set(
            key,
            ".",
            '{"foo": "bar", "baz" : 42}',
        )

        assert t.json_set(
            key,
            ".",
            '{"foo": "bar", "baz" : 42}',
            xx=True,
        )

    def test_json_get(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert t.json_set(key, ".", '{"foo": "bar", "baz" : 42}')
        assert (
            t.json_get(key, ".", format="XML", rootname="ROOT", arrname="ARR")
            == b'<?xml version="1.0" encoding="UTF-8"?><ROOT><foo>bar</foo><baz>42</baz></ROOT>'
        )

    def test_json_del(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert t.json_set(key, ".", '{"foo": "bar", "baz" : 42}')
        assert t.json_del(key, ".foo") == 1

    def test_json_type(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert t.json_set(key, ".", '{"foo": "bar", "baz" : 42}')
        assert t.json_type(key, ".foo") == "string"

    def test_json_numincrby(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert t.json_set(key, ".", '{"foo": "bar", "baz" : 42}')
        assert t.json_numincrby(key, ".baz", 10) == b"52"

    def test_json_strappend(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert t.json_set(key, ".", '{"foo": "bar", "baz" : 42}')
        assert t.json_strappend(key, ".foo", "rrrrr") == 8

    def test_json_strlen(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert t.json_set(key, ".", '{"foo": "bar", "baz" : 42}')
        assert t.json_strlen(key, ".foo") == 3

    def test_json_arrappend(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert t.json_set(key, ".", '{"id": [1,2,3]}')
        assert t.json_arrappend(key, ".id", ("null", "false", "true")) == 6

    def test_json_arrpop(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert t.json_set(key, ".", '{"id": [1,2,3]}')
        assert t.json_arrpop(key, ".id", 0) == b"1"

    def test_json_arrpop_no_index(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert t.json_set(key, ".", '{"id": [1,2,3]}')
        assert t.json_arrpop(key, ".id") == b"3"

    def test_json_arrinsert(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert t.json_set(key, ".", '{"id": [1,2,3]}')
        assert t.json_arrinsert(key, ".id", (10, 15), index=0) == 5

    def test_json_arrlen(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert t.json_set(key, ".", '{"id": [1,2,3]}')
        assert t.json_arrlen(key, ".id") == 3

    def test_json_arrtrim(self, t: Tair):
        key = "key_" + str(uuid.uuid4())

        assert t.json_set(key, ".", '{"id": [1,2,3,4,5,6]}')
        assert t.json_arrtrim(key, ".id", 3, 4) == 2
