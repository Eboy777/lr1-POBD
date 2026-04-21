import logging
import csv


class SQLManager:
    """Класс для работы с MySQL и PostgreSQL."""
    
    SUPPORTED_DBS = ('mysql', 'postgresql')
    SUPPORTED_OPERATORS = {
        '=', '!=', '<>', '>', '<', '>=', '<=', 'LIKE',
        'BETWEEN', 'IN'
    }

    def __init__(self, config, db_type='mysql', log_file='sql_queries.log'):
        self.config = config
        self.db_type = db_type.lower()
        if self.db_type not in self.SUPPORTED_DBS:
            raise ValueError(f"Unsupported db_type: {db_type}")
        self.connection = None
        self.cursor = None
        self._setup_logging(log_file)
    
    def _setup_logging(self, log_file):
        """Настройка логирования в файл"""
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format='%(asctime)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    def _log(self, query, params=()):
        """Записать запрос в лог"""
        logging.info(f"{query} | Params: {params}")

    def _to_dict_rows(self, rows):
        """Привести строки к словарям (для PostgreSQL при обычном курсоре)."""
        if not rows:
            return rows
        if isinstance(rows[0], dict):
            return rows
        if not self.cursor or not self.cursor.description:
            return rows
        columns = [desc[0] for desc in self.cursor.description]
        return [dict(zip(columns, row)) for row in rows]

    def _build_filter_clause(self, filters=None):
        """Собрать WHERE-условие и параметры из словаря фильтров."""
        if not filters:
            return '', []

        clauses = []
        params = []

        for column, condition in filters.items():
            operator = '='
            value = condition

            if isinstance(condition, tuple) and len(condition) == 2:
                operator, value = condition
            elif isinstance(condition, dict):
                operator = condition.get('op', '=')
                value = condition.get('value')

            operator = str(operator).upper().strip()
            if operator not in self.SUPPORTED_OPERATORS:
                raise ValueError(f"Unsupported operator: {operator}")

            if operator == 'BETWEEN':
                if not isinstance(value, (list, tuple)) or len(value) != 2:
                    raise ValueError(f"BETWEEN requires 2 values for '{column}'")
                clauses.append(f"{column} BETWEEN %s AND %s")
                params.extend([value[0], value[1]])
            elif operator == 'IN':
                if not isinstance(value, (list, tuple, set)):
                    raise ValueError(f"IN requires list/tuple/set for '{column}'")
                values = list(value)
                if not values:
                    clauses.append("1=0")
                else:
                    placeholders = ', '.join(['%s'] * len(values))
                    clauses.append(f"{column} IN ({placeholders})")
                    params.extend(values)
            else:
                clauses.append(f"{column} {operator} %s")
                params.append(value)

        return ' AND '.join(clauses), params
    
    def connect(self):
        """Установить соединение с БД"""
        if self.db_type == 'mysql':
            import mysql.connector
            self.connection = mysql.connector.connect(**self.config)
            self.cursor = self.connection.cursor(dictionary=True)
        else:
            try:
                import psycopg2
                from psycopg2.extras import RealDictCursor
                self.connection = psycopg2.connect(**self.config)
                self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            except ImportError:
                try:
                    import psycopg
                    self.connection = psycopg.connect(**self.config)
                    self.cursor = self.connection.cursor(row_factory=psycopg.rows.dict_row)
                except ImportError as exc:
                    raise ModuleNotFoundError(
                        "PostgreSQL driver is not installed. "
                        "Install one of: `pip install psycopg2-binary` or `pip install psycopg`"
                    ) from exc
        self._log("CONNECTED", ())
    
    def disconnect(self):
        """Закрыть соединение"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
    
    # === ТАБЛИЦЫ ===
    def create_table(self, name, columns):
        """Создать таблицу"""
        cols = ', '.join([f"{k} {v}" for k, v in columns.items()])
        query = f"CREATE TABLE IF NOT EXISTS {name} ({cols})"
        self._log(query, ())
        self.cursor.execute(query)
        self.connection.commit()
    
    def drop_table(self, table):
        """Удалить таблицу"""
        query = f"DROP TABLE IF EXISTS {table}"
        self._log(query, ())
        self.cursor.execute(query)
        self.connection.commit()
    
    def describe_table(self, table):
        """Вывести структуру таблицы"""
        if self.db_type == 'mysql':
            query = f"DESCRIBE {table}"
            params = ()
        else:
            query = (
                "SELECT column_name, data_type, is_nullable, column_default "
                "FROM information_schema.columns "
                "WHERE table_name = %s ORDER BY ordinal_position"
            )
            params = (table,)
        self._log(query, params)
        self.cursor.execute(query, params)
        return self._to_dict_rows(self.cursor.fetchall())
    
    # === КОЛОНКИ ===
    def add_column(self, table, name, dtype):
        """Добавить новый столбец"""
        query = f"ALTER TABLE {table} ADD {name} {dtype}"
        self._log(query, ())
        self.cursor.execute(query)
        self.connection.commit()
    
    def drop_column(self, table, name):
        """Удалить столбец"""
        query = f"ALTER TABLE {table} DROP COLUMN {name}"
        self._log(query, ())
        self.cursor.execute(query)
        self.connection.commit()
    
    # === INSERT ===
    def insert(self, table, data):
        """Вставить одну запись. Возвращает ID."""
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        self._log(query, list(data.values()))
        self.cursor.execute(query, list(data.values()))
        self.connection.commit()
        return getattr(self.cursor, 'lastrowid', None)
    
    def insert_many(self, table, rows):
        """Вставить несколько записей. Возвращает количество."""
        if not rows:
            return 0
        columns = ', '.join(rows[0].keys())
        placeholders = ', '.join(['%s'] * len(rows[0]))
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        values = [list(row.values()) for row in rows]
        self._log(query, values)
        self.cursor.executemany(query, values)
        self.connection.commit()
        return self.cursor.rowcount
    
    # === SELECT ===
    def select(self, table, columns='*', filters=None, order_by=None, limit=None):
        """Выбрать записи"""
        query = f"SELECT {columns} FROM {table}"
        where_clause, params = self._build_filter_clause(filters)
        if where_clause:
            query += f" WHERE {where_clause}"
        
        if order_by:
            query += f" ORDER BY {order_by}"
        
        if limit:
            query += f" LIMIT {limit}"
        
        self._log(query, params)
        self.cursor.execute(query, params)
        return self._to_dict_rows(self.cursor.fetchall())
    
    def select_one(self, table, columns='*', filters=None):
        """Выбрать одну запись"""
        results = self.select(table, columns, filters=filters, limit=1)
        return results[0] if results else None
    
    def select_sorted(self, table, column, order='ASC', filters=None):
        """Выбрать отсортированные записи (ASC или DESC)"""
        order = order.upper()
        if order not in ('ASC', 'DESC'):
            order = 'ASC'
        query = f"SELECT * FROM {table}"
        where_clause, params = self._build_filter_clause(filters)
        if where_clause:
            query += f" WHERE {where_clause}"
        query += f" ORDER BY {column} {order}"
        self._log(query, params)
        self.cursor.execute(query, params)
        return self._to_dict_rows(self.cursor.fetchall())
    
    def select_by_id_range(self, table, start_id, end_id, filters=None):
        """Выбрать диапазон строк по ID"""
        query = f"SELECT * FROM {table} WHERE id BETWEEN %s AND %s"
        params = [start_id, end_id]
        where_clause, where_params = self._build_filter_clause(filters)
        if where_clause:
            query += f" AND {where_clause}"
            params.extend(where_params)
        self._log(query, tuple(params))
        self.cursor.execute(query, params)
        return self._to_dict_rows(self.cursor.fetchall())
    
    def select_where_column(self, table, column, value, operator='=', filters=None):
        """Выбрать строки содержащие значение в столбце"""
        query_filters = {column: (operator, value)}
        if filters:
            query_filters.update(filters)
        return self.select(table, filters=query_filters)

    # === JOIN ===
    def select_join(
        self,
        left_table,
        right_table,
        on,
        join_type='INNER',
        columns='*',
        filters=None,
        order_by=None,
        limit=None
    ):
        """Универсальный JOIN-запрос: INNER/LEFT/RIGHT/FULL/CROSS."""
        join_type = {'FULL OUTER': 'FULL'}.get(join_type.upper(), join_type.upper())
        if join_type not in ('INNER', 'LEFT', 'RIGHT', 'FULL', 'CROSS'):
            raise ValueError(f"Unsupported join type: {join_type}")
        if join_type != 'CROSS' and not on:
            raise ValueError("Parameter 'on' is required for non-CROSS JOIN")

        query = f"SELECT {columns} FROM {left_table} {join_type} JOIN {right_table}"
        if on and join_type != 'CROSS':
            query += f" ON {on}"

        where_clause, params = self._build_filter_clause(filters)
        if where_clause:
            query += f" WHERE {where_clause}"
        if order_by:
            query += f" ORDER BY {order_by}"
        if limit:
            query += f" LIMIT {limit}"

        self._log(query, params)
        self.cursor.execute(query, params)
        return self._to_dict_rows(self.cursor.fetchall())

    # === UNION ===
    def select_union(self, select_specs, union_all=False, order_by=None, limit=None):
        """
        Выполнить UNION/UNION ALL.
        select_specs = [
            {'table': 'tours', 'columns': 'name', 'filters': {'price': ('>', 50000)}},
            {'table': 'hotels', 'columns': 'name', 'filters': {'rating': 5}}
        ]
        """
        if not select_specs or len(select_specs) < 2:
            raise ValueError("select_specs must contain at least 2 SELECT parts")

        parts = []
        params = []
        for spec in select_specs:
            table = spec['table']
            columns = spec.get('columns', '*')
            filters = spec.get('filters')

            part_query = f"SELECT {columns} FROM {table}"
            where_clause, part_params = self._build_filter_clause(filters)
            if where_clause:
                part_query += f" WHERE {where_clause}"
            parts.append(part_query)
            params.extend(part_params)

        union_keyword = 'UNION ALL' if union_all else 'UNION'
        query = f" {union_keyword} ".join(parts)
        if order_by:
            query += f" ORDER BY {order_by}"
        if limit:
            query += f" LIMIT {limit}"

        self._log(query, params)
        self.cursor.execute(query, params)
        return self._to_dict_rows(self.cursor.fetchall())
    
    # === UPDATE ===
    def update(self, table, data, filters=None):
        """Обновить записи"""
        set_clause = ', '.join([f"{k} = %s" for k in data.keys()])
        where_clause, where_params = self._build_filter_clause(filters)
        if not where_clause:
            raise ValueError("UPDATE requires at least one condition (filters)")
        query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
        params = list(data.values()) + where_params
        self._log(query, params)
        self.cursor.execute(query, params)
        self.connection.commit()
        return self.cursor.rowcount
    
    # === DELETE ===
    def delete(self, table, filters=None):
        """Удалить записи"""
        where_clause, where_params = self._build_filter_clause(filters)
        if not where_clause:
            raise ValueError("DELETE requires at least one condition (filters)")
        query = f"DELETE FROM {table} WHERE {where_clause}"
        self._log(query, where_params)
        self.cursor.execute(query, where_params)
        self.connection.commit()
        return self.cursor.rowcount
    
    def delete_by_id_range(self, table, start_id, end_id, filters=None):
        """Удалить диапазон строк по ID"""
        query = f"DELETE FROM {table} WHERE id BETWEEN %s AND %s"
        params = [start_id, end_id]
        where_clause, where_params = self._build_filter_clause(filters)
        if where_clause:
            query += f" AND {where_clause}"
            params.extend(where_params)
        self._log(query, tuple(params))
        self.cursor.execute(query, params)
        self.connection.commit()
        return self.cursor.rowcount
    
    # === CSV ===
    def export_to_csv(self, table, filename):
        """Экспорт таблицы в CSV"""
        rows = self.select(table)
        if not rows:
            return 0
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        
        return len(rows)
    
    def import_from_csv(self, table, filename):
        """Импорт таблицы из CSV"""
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        if not rows:
            return 0
        
        return self.insert_many(table, rows)

# ===  ИСПОЛЬЗОВАНИЯ ===
if __name__ == '__main__':
    db_config = {
        'user': 'postgres',
        'password': '1234',
        'host': 'localhost',
        'database': 'artemk',
        'port': 5432
    }
    
    # Оставьте True для полного автотеста (пересоздание таблиц + заполнение).
    # Поставьте False, если таблицы уже есть и хотите только проверить добавленные функции.
    RESET_AND_SEED = True

    with SQLManager(db_config, db_type='postgresql') as db:
        if RESET_AND_SEED:
            schema = {
                'clients': {'id': 'SERIAL PRIMARY KEY', 'name': 'VARCHAR(100)', 'email': 'VARCHAR(100)', 'phone': 'VARCHAR(20)'},
                'tours': {'id': 'SERIAL PRIMARY KEY', 'name': 'VARCHAR(200)', 'price': 'DECIMAL(10,2)', 'duration_days': 'INT'},
                'bookings': {'id': 'SERIAL PRIMARY KEY', 'client_id': 'INT', 'tour_id': 'INT', 'booking_date': 'DATE'},
                'hotels': {'id': 'SERIAL PRIMARY KEY', 'name': 'VARCHAR(150)', 'country': 'VARCHAR(100)', 'rating': 'INT'},
                'guides': {'id': 'SERIAL PRIMARY KEY', 'name': 'VARCHAR(100)', 'language': 'VARCHAR(50)', 'phone': 'VARCHAR(20)'},
                'transport': {'id': 'SERIAL PRIMARY KEY', 'type': 'VARCHAR(50)', 'capacity': 'INT', 'cost': 'DECIMAL(10,2)'},
                'destinations': {'id': 'SERIAL PRIMARY KEY', 'country': 'VARCHAR(100)', 'city': 'VARCHAR(100)', 'description': 'TEXT'},
                'payments': {'id': 'SERIAL PRIMARY KEY', 'booking_id': 'INT', 'amount': 'DECIMAL(10,2)', 'method': 'VARCHAR(50)'},
                'reviews': {'id': 'SERIAL PRIMARY KEY', 'client_id': 'INT', 'tour_id': 'INT', 'rating': 'INT'}
            }
            data = {
                'clients': [
                    {'name': 'Иван Петров', 'email': 'ivan@test.com', 'phone': '+79001234567'},
                    {'name': 'Мария Сидорова', 'email': 'maria@test.com', 'phone': '+79009876543'},
                    {'name': 'Алексей Иванов', 'email': 'alex@test.com', 'phone': '+79005554433'},
                    {'name': 'Елена Козлова', 'email': 'elena@test.com', 'phone': '+79003332211'}
                ],
                'tours': [
                    {'name': 'Отдых в Турции', 'price': 50000.00, 'duration_days': 7},
                    {'name': 'Тур в Египет', 'price': 45000.00, 'duration_days': 10},
                    {'name': 'Отдых в Таиланде', 'price': 80000.00, 'duration_days': 14},
                    {'name': 'Евротур', 'price': 120000.00, 'duration_days': 21}
                ],
                'bookings': [
                    {'client_id': 1, 'tour_id': 1, 'booking_date': '2025-06-15'},
                    {'client_id': 2, 'tour_id': 2, 'booking_date': '2025-07-01'},
                    {'client_id': 3, 'tour_id': 3, 'booking_date': '2025-08-10'},
                    {'client_id': 4, 'tour_id': 4, 'booking_date': '2025-09-05'}
                ],
                'hotels': [
                    {'name': 'Grand Hotel 5*', 'country': 'Турция', 'rating': 5},
                    {'name': 'Sea Resort 4*', 'country': 'Египет', 'rating': 4},
                    {'name': 'Tropical Paradise 5*', 'country': 'Таиланд', 'rating': 5},
                    {'name': 'City Center 3*', 'country': 'Италия', 'rating': 3}
                ],
                'guides': [
                    {'name': 'Ахмед Хасан', 'language': 'арабский', 'phone': '+201001112233'},
                    {'name': 'Ольга Смирнова', 'language': 'русский', 'phone': '+79001112233'},
                    {'name': 'John Smith', 'language': 'английский', 'phone': '+442012345678'},
                    {'name': 'Мария Гончарова', 'language': 'испанский', 'phone': '+34911223344'}
                ],
                'transport': [
                    {'type': 'Самолёт', 'capacity': 200, 'cost': 15000.00},
                    {'type': 'Автобус', 'capacity': 50, 'cost': 5000.00},
                    {'type': 'Микроавтобус', 'capacity': 18, 'cost': 3000.00},
                    {'type': 'Яхта', 'capacity': 12, 'cost': 25000.00}
                ],
                'destinations': [
                    {'country': 'Турция', 'city': 'Анталья', 'description': 'Курортный город на Средиземном море'},
                    {'country': 'Египет', 'city': 'Шарм-эль-Шейх', 'description': 'Курорт на Красном море'},
                    {'country': 'Таиланд', 'city': 'Пхукет', 'description': 'Остров в Андаманском море'},
                    {'country': 'Италия', 'city': 'Рим', 'description': 'Столица Италии, город вечности'}
                ],
                'payments': [
                    {'booking_id': 1, 'amount': 50000.00, 'method': 'карта'},
                    {'booking_id': 2, 'amount': 45000.00, 'method': 'наличные'},
                    {'booking_id': 3, 'amount': 80000.00, 'method': 'перевод'},
                    {'booking_id': 4, 'amount': 120000.00, 'method': 'карта'}
                ],
                'reviews': [
                    {'client_id': 1, 'tour_id': 1, 'rating': 5},
                    {'client_id': 2, 'tour_id': 2, 'rating': 4},
                    {'client_id': 3, 'tour_id': 3, 'rating': 5},
                    {'client_id': 4, 'tour_id': 4, 'rating': 3}
                ]
            }
            tables = ['clients', 'tours', 'bookings', 'hotels', 'guides', 'transport', 'destinations', 'payments', 'reviews']
            for table in reversed(tables):
                db.drop_table(table)
            for table in tables:
                db.create_table(table, schema[table])
                db.insert_many(table, data[table])
            print("Заполнено данными 9 таблиц")
        # Если не нужно пересоздавать и заполнять таблицы, просто используйте RESET_AND_SEED = False.

        print("\nФильтрация (price >= 50000 и duration 7..14):")
        filtered_tours = db.select(
            'tours',
            filters={
                'price': ('>=', 50000),
                'duration_days': ('BETWEEN', (7, 14))
            },
            order_by='price DESC'
        )
        for row in filtered_tours:
            print(row)

        print("\nINNER JOIN bookings + clients:")
        inner_rows = db.select_join(
            'bookings b',
            'clients c',
            on='b.client_id = c.id',
            join_type='INNER',
            columns='b.id AS booking_id, c.name AS client_name, b.booking_date',
            order_by='b.id'
        )
        for row in inner_rows:
            print(row)

        print("\nLEFT JOIN clients + reviews:")
        left_rows = db.select_join(
            'clients c',
            'reviews r',
            on='c.id = r.client_id',
            join_type='LEFT',
            columns='c.name, r.rating',
            order_by='c.id'
        )
        for row in left_rows:
            print(row)

        print("\nRIGHT JOIN bookings + tours:")
        right_rows = db.select_join(
            'bookings b',
            'tours t',
            on='b.tour_id = t.id',
            join_type='RIGHT',
            columns='t.name AS tour_name, b.booking_date',
            order_by='t.id'
        )
        for row in right_rows:
            print(row)

        print("\nFULL JOIN reviews + tours:")
        full_rows = db.select_join(
            'reviews r',
            'tours t',
            on='r.tour_id = t.id',
            join_type='FULL',
            columns='t.name AS tour_name, r.rating',
            order_by='tour_name'
        )
        for row in full_rows:
            print(row)

        print("\nCROSS JOIN clients + transport (5 строк):")
        cross_rows = db.select_join(
            'clients c',
            'transport tr',
            on=None,
            join_type='CROSS',
            columns='c.name AS client_name, tr.type AS transport_type',
            limit=5
        )
        for row in cross_rows:
            print(row)

        print("\nUNION ALL clients + guides:")
        union_rows = db.select_union(
            [
                {
                    'table': 'clients',
                    'columns': "name AS entity_name, 'client' AS source",
                    'filters': {'id': ('IN', [1, 2])}
                },
                {
                    'table': 'guides',
                    'columns': "name AS entity_name, 'guide' AS source",
                    'filters': {'id': ('IN', [1, 2])}
                }
            ],
            union_all=True,
            order_by='entity_name'
        )
        for row in union_rows:
            print(row)
