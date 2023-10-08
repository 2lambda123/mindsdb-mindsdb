# check if weaviate is installed
import importlib
from unittest.mock import patch

import pandas as pd
import pytest
from mindsdb_sql import parse_sql

from ..unit.executor_test_base import BaseExecutorTest

try:
    importlib.import_module("weaviate")
    WEAVIATE_INSTALLED = True
except ImportError:
    WEAVIATE_INSTALLED = False


@pytest.mark.parametrize(
    "url, api_key",
    [
        (
            "https://sample-0tz79bia.weaviate.network",
            "YOwV62Sdb1fGTNAHejwKHFallJY6SQM6gCOc",
        )
    ],
)
def input_credentials(url, api_key):
    return url, api_key


@pytest.mark.skipif(not WEAVIATE_INSTALLED, reason="weaviate is not installed")
class TestWeaviateHandler(BaseExecutorTest):
    def run_sql(self, sql):
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))

        assert ret.error_code is None
        if ret.data is not None:
            columns = [
                col.alias if col.alias is not None else col.name for col in ret.columns
            ]
            return pd.DataFrame(ret.data, columns=columns)

    def setup_method(self):
        super().setup_method()
        # create a weaviate database connection
        url, api_key = input_credentials
        self.run_sql(
            f"""
            CREATE DATABASE weaviate_test
            WITH ENGINE = "weaviate",
            PARAMETERS = {{
                "weaviate_url" : "{url}",
                "weaviate_api_key": "{api_key}"
            }}
        """
        )

    @pytest.mark.xfail(reason="create table for vectordatabase is not well supported")
    @patch("mindsdb.integrations.handlers.weaviate_handler.Handler")
    def test_create_table(self, weaviate_handler_mock):
        # create an empty table
        sql = """
            CREATE TABLE weaviate_test.test_table;
        """
        self.run_sql(sql)

        # create a table with the schema definition is not allowed

        sql = """
            CREATE TABLE weaviate_test.test_table (
                id int,
                metadata text,
                embedding float[]
            );
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

    @patch("mindsdb.integrations.handlers.weaviate_handler.Handler")
    def test_create_with_select(self, weaviate_handler_mock):
        df = pd.DataFrame(
            {
                "id": ["id1", "id2"],
                "content": ["this is a test", "this is a test"],
                "metadata": [{"test": "test"}, {"test": "test"}],
                "embeddings": [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0]],
            }
        )

        self.set_handler(weaviate_handler_mock, "weaviate", tables={"test_table": df})

        sql = """
        CREATE TABLE weaviate_test.test_table2 (
            SELECT * FROM weaviate.df
        )
        """
        # this should work
        self.run_sql(sql)

    @pytest.mark.xfail(reason="drop table for vectordatabase is not working")
    @patch("mindsdb.integrations.handlers.weaviate_handler.Handler")
    def test_drop_table(self, weaviate_handler_mock):
        df = pd.DataFrame(
            {
                "id": ["id1", "id2"],
                "content": ["this is a test", "this is a test"],
                "metadata": [{"test": "test"}, {"test": "test"}],
                "embeddings": [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0]],
            }
        )
        self.set_handler(weaviate_handler_mock, "weaviate", tables={"test_table": df})

        # create a table
        sql = """
            CREATE TABLE weaviate_test.test_table (
                SELECT * FROM weaviate.df
            )
        """
        self.run_sql(sql)

        # drop a table
        sql = """
            DROP TABLE weaviate_test.test_table;
        """
        self.run_sql(sql)

        # drop a non existent table will raise an error
        sql = """
            DROP TABLE weaviate_test.test_table2;
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

    @patch("mindsdb.integrations.handlers.weaviate_handler.Handler")
    def test_insert_into(self, weaviate_handler_mock):
        df = pd.DataFrame(
            {
                "id": ["id1", "id2", "id3"],
                "content": ["this is a test", "this is a test", "this is a test"],
                "metadata": [{"test": "test1"}, {"test": "test2"}, {"test": "test3"}],
                "embeddings": [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0], [1.0, 2.0, 3.0]],
            }
        )
        df2 = pd.DataFrame(
            {
                "id": ["id1", "id2", "id3"],
                "content": ["this is a test", "this is a test", "this is a test"],
                "metadata": [{"test": "test1"}, {"test": "test2"}, {"test": "test3"}],
                "embeddings": [
                    [1.0, 2.0, 3.0, 4.0],
                    [1.0, 2.0],
                    [1.0, 2.0, 3.0],
                ],  # different dimensions
            }
        )
        self.set_handler(
            weaviate_handler_mock, "weaviate", tables={"df": df, "df2": df2}
        )
        num_record = df.shape[0]

        # create a table
        sql = """
            CREATE TABLE weaviate_test.test_table (
                SELECT * FROM weaviate.df
            )
        """
        self.run_sql(sql)

        # insert into a table with values
        sql = """
            INSERT INTO weaviate_test.test_table (
                id,content,metadata,embeddings
            )
            VALUES (
                'some_unique_id', 'this is a test', '{"test": "test"}', '[1.0, 2.0, 3.0]'
            )
        """
        self.run_sql(sql)
        # check if the data is inserted
        sql = """
            SELECT * FROM weaviate_test.test_table
            WHERE id = 'some_unique_id'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 1

        # insert without specifying id should also work
        sql = """
            INSERT INTO weaviate_test.test_table (
                content,metadata,embeddings
            )
            VALUES (
                'this is a test', '{"test": "test"}', '[1.0, 2.0, 3.0]'
            )
        """
        self.run_sql(sql)
        # check if the data is inserted
        sql = """
            SELECT * FROM weaviate_test.test_table
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == num_record + 2

        # insert into a table with a select statement
        sql = """
            INSERT INTO weaviate_test.test_table (
                content,metadata,embeddings
            )
            SELECT
                content,metadata,embeddings
            FROM
                weaviate.df
        """
        self.run_sql(sql)
        # check if the data is inserted
        sql = """
            SELECT * FROM weaviate_test.test_table
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == num_record * 2 + 2

        # insert into a table with a select statement, but wrong columns
        with pytest.raises(Exception):
            sql = """
                INSERT INTO weaviate_test.test_table
                SELECT
                    content,metadata,embeddings as wrong_column
                FROM
                    weaviate.df
            """
            self.run_sql(sql)

        # insert into a table with a select statement, missing metadata column
        sql = """
            INSERT INTO weaviate_test.test_table
            SELECT
                content,embeddings
            FROM
                weaviate.df
        """
        self.run_sql(sql)

        # insert into a table with a select statement, missing embedding column, shall raise an error
        with pytest.raises(Exception):
            sql = """
                INSERT INTO weaviate_test.test_table
                SELECT
                    content,metadata
                FROM
                    weaviate.df
            """
            self.run_sql(sql)

        # insert into a table with a select statement, with different embedding dimensions, shall raise an error
        sql = """
            INSERT INTO weaviate_test.test_table
            SELECT
                content,metadata,embeddings
            FROM
                weaviate.df2
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

        # insert into a table with existing id, shall raise an error
        sql = """
            INSERT INTO weaviate_test.test_table (
                id,content,metadata,embeddings
            )
            VALUES (
                'id1', 'this is a test', '{"test": "test"}', '[1.0, 2.0, 3.0]'
            )
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

    @patch("mindsdb.integrations.handlers.weaviate_handler.Handler")
    def test_select_from(self, weaviate_handler_mock):
        df = pd.DataFrame(
            {
                "id": ["id1", "id2"],
                "content": ["this is a test", "this is a test"],
                "metadata": [{"test": "test"}, {"test": "test"}],
                "embeddings": [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0]],
            }
        )
        self.set_handler(weaviate_handler_mock, "weaviate", tables={"test_table": df})
        # create a table
        sql = """
            CREATE TABLE weaviate_test.test_table (
                SELECT * FROM weaviate.df
            )
        """
        self.run_sql(sql)

        # query a table without any filters
        sql = """
            SELECT * FROM weaviate_test.test_table
        """
        self.run_sql(sql)

        # query a table with id
        sql = """
            SELECT * FROM weaviate_test.test_table
            WHERE id = 'id1'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 1

        # query a table with a search vector, without limit
        sql = """
            SELECT * FROM weaviate_test.test_table
            WHERE search_vector = '[1.0, 2.0, 3.0]'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        # query a table with a search vector, with limit
        sql = """
            SELECT * FROM weaviate_test.test_table
            WHERE search_vector = '[1.0, 2.0, 3.0]'
            LIMIT 1
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 1

        # query a table with a metadata filter
        sql = """
            SELECT * FROM weaviate_test.test_table
            WHERE `metadata.test` = 'test'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        # query a table with a metadata filter and a search vector
        sql = """
            SELECT * FROM weaviate_test.test_table
            WHERE `metadata.test` = 'test'
            AND search_vector = '[1.0, 2.0, 3.0]'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

    @pytest.mark.xfail(reason="upsert for vectordatabase is not implemented")
    def test_update(self):
        # update a table with a metadata filter
        sql = """
            UPDATE weaviate_test.test_table
            SET `metadata.test` = 'test2'
            WHERE `metadata.test` = 'test'
        """
        self.run_sql(sql)
        # check if the data is updated
        sql = """
            SELECT * FROM weaviate_test.test_table
            WHERE `metadata.test` = 'test2'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        # update the embeddings
        sql = """
            UPDATE weaviate_test.test_table
            SET embedding = [3.0, 2.0, 1.0]
            WHERE `metadata.test` = 'test2'
        """
        self.run_sql(sql)
        # check if the data is updated
        sql = """
            SELECT * FROM weaviate_test.test_table
            WHERE `metadata.test` = 'test2'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2
        assert ret.embedding[0] == [3.0, 2.0, 1.0]

        # update multiple columns
        sql = """
            UPDATE weaviate_test.test_table
            SET `metadata.test` = 'test3',
                embedding = [1.0, 2.0, 3.0]
                content = 'this is a test'
            WHERE `metadata.test` = 'test2'
        """
        self.run_sql(sql)
        # check if the data is updated
        sql = """
            SELECT * FROM weaviate_test.test_table
            WHERE `metadata.test` = 'test3'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2
        assert ret.embedding[0] == [1.0, 2.0, 3.0]
        assert ret.content[0] == "this is a test"

        # update a table with a search vector filter is not allowed
        sql = """
            UPDATE weaviate_test.test_table
            SET `metadata.test = 'test2'
            WHERE search_vector = [1.0, 2.0, 3.0]
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

        # update a table without any filters is allowed
        sql = """
            UPDATE weaviate_test.test_table
            SET metadata.test = 'test3'
        """
        self.run_sql(sql)
        # check if the data is updated
        sql = """
            SELECT * FROM weaviate_test.test_table
            WHERE `metadata.test` = 'test3'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        # update a table with a search vector filter and a metadata filter is not allowed
        sql = """
            UPDATE weaviate_test.test_table
            SET metadata.test = 'test3'
            WHERE metadata.test = 'test2'
            AND search_vector = [1.0, 2.0, 3.0]
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

    @patch("mindsdb.integrations.handlers.weaviate_handler.Handler")
    def test_delete(self, weaviate_handler_mock):
        df = pd.DataFrame(
            {
                "id": ["id1", "id2"],
                "content": ["this is a test", "this is a test"],
                "metadata": [{"test": "test1"}, {"test": "test2"}],
                "embeddings": [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0]],
            }
        )
        self.set_handler(weaviate_handler_mock, "weaviate", tables={"test_table": df})

        # create a table
        sql = """
            CREATE TABLE weaviate_test.test_table (
                SELECT * FROM weaviate.df
            )
        """
        self.run_sql(sql)

        # delete from a table with a metadata filter
        sql = """
            DELETE FROM weaviate_test.test_table
            WHERE `metadata.test` = 'test1'
        """
        self.run_sql(sql)
        # check if the data is deleted
        sql = """
            SELECT * FROM weaviate_test.test_table
            WHERE `metadata.test` = 'test2'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 1

        # delete by id
        sql = """
            DELETE FROM weaviate_test.test_table
            WHERE id = 'id2'
        """
        self.run_sql(sql)
        # check if the data is deleted
        sql = """
            SELECT * FROM weaviate_test.test_table
            WHERE id = 'id2'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 0

        # delete from a table with a search vector filter is not allowed
        sql = """
            DELETE FROM weaviate_test.test_table
            WHERE search_vector = [1.0, 2.0, 3.0]
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

        # delete from a table without any filters is not allowed
        sql = """
            DELETE FROM weaviate_test.test_table
        """
        with pytest.raises(Exception):
            self.run_sql(sql)
