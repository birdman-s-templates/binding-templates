import psycopg2
from psycopg2._psycopg import connection


class PostgresClient:

    def __init__(
        self,
        host: str = None,
        port: str = None,
        user: str = None,
        password: str = None,
        database_name: str = None
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database_name = database_name

        # Здесь будут храниться текущие conn и cursor
        self._conn: connection | None = None
        self._cursor = None

    def __enter__(self) -> "PostgresClient":
        """
        При входе в 'with' сразу открываем новое соединение и курсор.
        """
        self._conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database_name
        )
        self._cursor = self._conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        При выходе из 'with' — закрываем курсор и соединение.
        Если хотите, можно здесь сделать rollback при исключении:
            if exc_type:
                self._conn.rollback()
            else:
                self._conn.commit()
        Но в простом варианте достаточно commit() внутри execute()
        (а для SELECT commit() не нужен).
        """
        if self._cursor:
            try:
                self._cursor.close()
            except Exception:
                pass
        if self._conn:
            try:
                if exc_type:
                    self._conn.rollback()
                else:
                    self._conn.commit()
                self._conn.close()
            except Exception:
                pass

    def select(self, text_query: str) -> "PostgresClient.QueryResult":
        """
        Выполняет SELECT, возвращает QueryResult.
        """
        if not self._conn or not self._cursor:
            raise RuntimeError("Соединение не открыто. Используйте with PostgresClient(...).")

        self._cursor.execute(text_query)
        column_names = [col[0] for col in self._cursor.description]
        fetched = self._cursor.fetchall()

        res = PostgresClient.QueryResult()
        res.rowcount = len(fetched)
        if fetched:
            res.first_row = dict(zip(column_names, fetched[0]))
        else:
            res.first_row = {}

        for row in fetched:
            res.rows.append(dict(zip(column_names, row)))

        return res

    def execute(self, text_query: str, params=None) -> int:
        """
        Выполняет INSERT/UPDATE/DELETE, возвращает rowcount и сразу делает COMMIT.
        """
        if not self._conn or not self._cursor:
            raise RuntimeError("Соединение не открыто. Используйте with PostgresClient(...).")

        if params is None:
            self._cursor.execute(text_query)
            cnt = self._cursor.rowcount
            self._conn.commit()
        else:
            self._cursor.execute(text_query, params)
            cnt = self._cursor.rowcount
            self._conn.commit()
        return cnt

    def executemany(self, text_query: str, data: list[tuple]) -> int:
        """
        Пакетная вставка/обновление: возвращает rowcount последней операции и делает COMMIT.
        """
        if not self._conn or not self._cursor:
            raise RuntimeError("Соединение не открыто. Используйте with PostgresClient(...).")

        self._cursor.executemany(text_query, data)
        cnt = self._cursor.rowcount
        self._conn.commit()
        return cnt

    def fetchone(self, text_query: str) -> dict:
        """
        Выполняет запрос, возвращает первую строку (или {}).
        """
        if not self._conn or not self._cursor:
            raise RuntimeError("Соединение не открыто. Используйте with PostgresClient(...).")

        self._cursor.execute(text_query)
        row = self._cursor.fetchone()
        if not row:
            return {}
        columns = [col[0] for col in self._cursor.description]
        return dict(zip(columns, row))

    def get_column_values(self, table_name: str, column_name: str, schema_name=None, limit=None) -> list:
        """
        Возвращает список значений из указанной колонки таблицы с ограничением количества строк.

        Метод проверяет существование колонки в таблице (с учетом схемы, если указана),
        затем выполняет запрос для выборки значений.

        :param table_name: Название таблицы.
        :param column_name: Название целевой колонки.
        :param schema_name: Название схемы (опционально, по умолчанию используется search_path).
        :param limit: Максимальное количество строк для выборки. Если None, выбираются все строки.
        :return: Список значений из колонки (все строки, включая возможные дубликаты).
        """
        if not self._conn or not self._cursor:
            raise RuntimeError("Соединение не открыто. Используйте with PostgresClient(...).")

        # Проверяем существование таблицы и колонки (с учетом схемы, если указана)
        check_query = """
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = %s AND column_name = %s
        """
        params = [table_name, column_name]
        if schema_name:
            check_query += " AND table_schema = %s"
            params.append(schema_name)

        self._cursor.execute(check_query, params)
        if not self._cursor.fetchone():
            schema_str = f" в схеме '{schema_name}'" if schema_name else ""
            raise ValueError(f"Колонка '{column_name}' не найдена в таблице '{table_name}'{schema_str}.")

        # Строим SQL-запрос для выборки значений, с учётом схемы и лимита
        full_table_name = f"{schema_name}.{table_name}" if schema_name else table_name
        query = f"SELECT {column_name} FROM {full_table_name}"
        if limit is not None:
            query += f" LIMIT {limit}"

        self._cursor.execute(query)
        return [row[0] for row in self._cursor.fetchall()]

    class QueryResult:
        """
        Результат SELECT-запроса.
        """
        def __init__(self) -> None:
            self.rowcount: int = 0
            self.first_row: dict = {}
            self.rows: list[dict] = []
