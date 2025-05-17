# to_sql의 name 인자는 테이블 명
## ex) name = f"{code}_min_chart"

from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text
# mysql과 연결하기 위해 사용하는 함수
import mysql.connector
# DB가 아니라 MySQL과 연결하기 위해 사용하는 함수

# 메모리 사용량을 측정하기 위해 사용하는 함수
from memory_profiler import profile

import pymysql
pymysql.install_as_MySQLdb()

import pandas as pd

import warnings
warnings.filterwarnings('ignore')



class manage_db:
    def __init__(self, localhost, username, password, database):
        self.localhost = localhost
        self.username = username
        self.password = password
        self.database = database
        self.conn = None # 데이터 베이스 연결 객체를 저장할 변수
        self.cur = None# 데이터 베이스 커서 객체를 저장할 변수
        self.connect_sql() # 데이터 베이스에 연결을 시도

        self.engine = create_engine(f"mysql+pymysql://{username}:{password}@{localhost}/{database}")

    def connect_sql(self):
        """데이터 베이스에 연결"""
        try:
            self.conn = mysql.connector.connect(host=self.localhost, user=self.username, password=self.password, db=self.database, charset='utf8')
            self.cur = self.conn.cursor()
            print("Connected to the database")
        except pymysql.MySQLError as e:
            # 연결 중 오류가 발생할 경우 예외 출력
            print(f"Error connecting to the database: {e}")

    def close_sql(self):
        """데이터 베이스 연결 해제"""
        if self.cur:
            self.cur.close() # 커서가 존재하면 커서를 닫는다.
        if self.conn:
            self.conn.close() # 데이터 베이스 연결이 존재하면 연결을 닫는다.
        print("Disconnected to the database")

    def table_exists(self, table):
        """테이블 존재 여부 확인"""
        inspector = inspect(self.engine)
        return table in inspector.get_table_names()

    def insert_data(self, table, dataframe, unique_col, chunksize=1000):
        """
        데이터를 삽입 and 없으면 table 생성 후 삽입
        :param table(code): 데이터를 삽입할 테이블 이름 (str)
        :param dataframe: 삽입할 데이터 프레임(pd.DataFrame)
        :param unique_column: 중복 확인에 사용할 컬럼명 (str)
        :param chunksize: 한 번에 삽입할 데이터 양 (int)
        """
        # table 존재 여부 확인
        table_exists = self.table_exists(table)

        if table_exists is False:
            dataframe.to_sql(name=f'{table}', con=self.engine, if_exists='replace', index=False, chunksize=chunksize)
            print(f"Table {table} created and data inserted.")
        else:
            # 테이블이 존재하면 임시 테이블에 데이터프레임 삽입
            temp_table = f"temp_{table}"
            dataframe.to_sql(name=f'{temp_table}', con=self.engine, if_exists='replace', index=False, chunksize=chunksize)
            # values = ', '.join(
            #     f"({', '.join([repr(x) for x in row])})" for row in dataframe.values
            # )
            # date 컬럼을 기준으로 중복 확인 후 새로운 날짜 데이터만 삽입

            insert_query = f"""
                INSERT INTO {table}
                SELECT *
                FROM {temp_table};
                """

            try:
                # 중복 확인 후 새로운 데이터만 삽입
                with self.engine.connect() as connection:
                    connection.execute(text(insert_query))
                print(f"Data inserted into table {table}.")
            except Exception as e:
                print(f"Error: {e}")
            finally:
                # 임시 테이블 삭제
                drop_query = f"DROP TABLE IF EXISTS {temp_table}"
                with self.engine.connect() as connection:
                    connection.execute(text(drop_query))
                print(f"Temporary table {temp_table} dropped.")

    def fetch_data(self, table, columns='*', condition=None):
        """
        테이블에서 데이터를 조회하여 데이터프레임으로 반환합니다.
        :param table: 조회할 테이블 이름 (str)
        :param columns: 조회할 컬럼명 (str 또는 list), 기본값은 모든 컬럼
        :param condition: 조회 조건 (str), 기본값은 없음
        :return: 조회된 데이터가 담긴 데이터프레임
        """
        # 컬럼 선택 옵션 처리
        if isinstance(columns, list):
            columns = ', '.join(columns)

        # 기본 SELECT 쿼리 생성
        query = f"SELECT {columns} FROM {table}"
        if condition:
            query += f" WHERE {condition}"

        try:
            # 쿼리 실행 후 결과를 데이터프레임으로 반환
            data_frame = pd.read_sql(query, self.engine)
            print("Data fetched successfully.")
            return data_frame
        except Exception as e:
            print("Error fetching data:", e)
            return None