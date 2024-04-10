import pandas as pd
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb_sql.parser import ast
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from typing import List, Tuple
from math import ceil
from utils.cryptopanic_api import call_cryptopanic_api

DEFAULT_NUMBER_OF_NEWS=200

class NewsTable(APITable):
    def select(self, query: ast.Select) -> pd.DataFrame:
        """Select data from the news table and return it as a pandas DataFrame.
        Args:
            query (ast.Select): The SQL query to be executed.
        Returns:
            pandas.DataFrame: A pandas DataFrame containing the selected data.
        """

        # Extract the limit value from the SQL query, if it exists
        limit = None
        if query.limit is not None:
            limit = query.limit.value

        api_condition={}

        # Apply any WHERE clauses in the SQL query to the DataFrame
        conditions = extract_comparison_conditions(query.where)
        for condition in conditions:
            if condition[0] == '=' and condition[1] == 'filters':
                api_condition['filters'] = condition[2] if isinstance(condition[2], str) else condition[2].join(',')
                conditions.remove(condition)
            elif condition[0] == '=' and condition[1] == 'currency':
                api_condition['currencies'] = condition[2] if isinstance(condition[2], str) else condition[2].join(',')
                conditions.remove(condition)
            elif condition[0] == '=' and condition[1] == 'regions':
                api_condition['regions'] = condition[2] if isinstance(condition[2], str) else condition[2].join(',')
                conditions.remove(condition)
            elif condition[0] == '=' and condition[1] == 'kind':
                api_condition['kind'] = condition[2] 
                conditions.remove(condition)
            elif condition[0] == '=' and condition[1] == 'following':
                api_condition['following'] = condition[2]
                conditions.remove(condition)

                       
        api_condition['lookback'] = ceil(DEFAULT_NUMBER_OF_NEWS // 200) if limit is None else  ceil(limit // 200)
        api_condition['api_token'] = self.handler.api_token

        df = call_cryptopanic_api(**api_condition)


        # Filter the columns in the DataFrame according to the SQL query
        self.filter_columns(df, query)

        return df


    def get_columns(self):
        """Get the list of column names for the stories table.
        Returns:
            list: A list of column names for the stories table.
        """
        return self.columns

    def filter_columns(self, df, query):
        """Filter the columns in the DataFrame according to the SQL query.
        Args:
            df (pandas.DataFrame): The DataFrame to filter.
            query (ast.Select): The SQL query to apply to the DataFrame.
        """
        columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                columns.append(target.value)
        df = df[columns]
        return df
 