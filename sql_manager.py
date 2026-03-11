import mysql.connector
import logging
import csv


class SQLManager:
    """класс для работы с MySQL"""
    
    def __init__(self, config, log_file='sql_queries.log'):
        self.config = config
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
    
    def connect(self):
        """Установить соединение с БД"""
        self.connection = mysql.connector.connect(**self.config)
        self.cursor = self.connection.cursor(dictionary=True)
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
        """Вывести структуру таблицы (DESCRIBE)"""
        query = f"DESCRIBE {table}"
        self._log(query, ())
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
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
        return self.cursor.lastrowid
    
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
    def select(self, table, columns='*', where=None, order_by=None, limit=None):
        """Выбрать записи"""
        query = f"SELECT {columns} FROM {table}"
        params = []
        
        if where:
            conditions = ' AND '.join([f"{k} = %s" for k in where.keys()])
            query += f" WHERE {conditions}"
            params = list(where.values())
        
        if order_by:
            query += f" ORDER BY {order_by}"
        
        if limit:
            query += f" LIMIT {limit}"
        
        self._log(query, params)
        self.cursor.execute(query, params)
        return self.cursor.fetchall()
    
    def select_one(self, table, columns='*', where=None):
        """Выбрать одну запись"""
        results = self.select(table, columns, where, limit=1)
        return results[0] if results else None
    
    def select_sorted(self, table, column, order='ASC'):
        """Выбрать отсортированные записи (ASC или DESC)"""
        order = order.upper()
        if order not in ('ASC', 'DESC'):
            order = 'ASC'
        query = f"SELECT * FROM {table} ORDER BY {column} {order}"
        self._log(query, ())
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def select_by_id_range(self, table, start_id, end_id):
        """Выбрать диапазон строк по ID"""
        query = f"SELECT * FROM {table} WHERE id BETWEEN %s AND %s"
        self._log(query, (start_id, end_id))
        self.cursor.execute(query, (start_id, end_id))
        return self.cursor.fetchall()
    
    def select_where_column(self, table, column, value):
        """Выбрать строки содержащие значение в столбце"""
        query = f"SELECT * FROM {table} WHERE {column} = %s"
        self._log(query, (value,))
        self.cursor.execute(query, (value,))
        return self.cursor.fetchall()
    
    # === UPDATE ===
    def update(self, table, data, where):
        """Обновить записи"""
        set_clause = ', '.join([f"{k} = %s" for k in data.keys()])
        where_clause = ' AND '.join([f"{k} = %s" for k in where.keys()])
        query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
        self._log(query, list(data.values()) + list(where.values()))
        self.cursor.execute(query, list(data.values()) + list(where.values()))
        self.connection.commit()
        return self.cursor.rowcount
    
    # === DELETE ===
    def delete(self, table, where):
        """Удалить записи"""
        where_clause = ' AND '.join([f"{k} = %s" for k in where.keys()])
        query = f"DELETE FROM {table} WHERE {where_clause}"
        self._log(query, list(where.values()))
        self.cursor.execute(query, list(where.values()))
        self.connection.commit()
        return self.cursor.rowcount
    
    def delete_by_id_range(self, table, start_id, end_id):
        """Удалить диапазон строк по ID"""
        query = f"DELETE FROM {table} WHERE id BETWEEN %s AND %s"
        self._log(query, (start_id, end_id))
        self.cursor.execute(query, (start_id, end_id))
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


# === ПРИМЕР ИСПОЛЬЗОВАНИЯ ===
if __name__ == '__main__':
    db_config = {
        'user': 'j30084097_13418',
        'password': 'pPS090207/()',
        'host': 'srv221-h-st.jino.ru',
        'database': 'j30084097_13418'
    }
    
    with SQLManager(db_config) as db:
        # Удалить старую таблицу если существует
        db.drop_table('users777')
        
        # Создать таблицу users777 с 5 строками
        db.create_table('users777', {
            'id': 'INT AUTO_INCREMENT PRIMARY KEY',
            'name': 'VARCHAR(100)',
            'email': 'VARCHAR(100)',
            'age': 'INT'
        })
        
        # Вставка 5 записей
        db.insert('users777', {'name': 'Alex', 'email': 'alex@test.com', 'age': 25})
        db.insert('users777', {'name': 'Bob', 'email': 'bob@test.com', 'age': 30})
        db.insert('users777', {'name': 'Anna', 'email': 'anna@test.com', 'age': 22})
        db.insert('users777', {'name': 'Mike', 'email': 'mike@test.com', 'age': 28})
        db.insert('users777', {'name': 'Kate', 'email': 'kate@test.com', 'age': 27})
        
        print("Таблица создана и заполнена 5 строками")
        
        # 1. Вывод столбца по возрастанию/убыванию
        users_asc = db.select_sorted('users777', 'name', 'ASC')
        print(f"\n1. Сортировка по имени (ASC): {[u['name'] for u in users_asc]}")
        users_desc = db.select_sorted('users777', 'age', 'DESC')
        print(f"1. Сортировка по возрасту (DESC): {[u['age'] for u in users_desc]}")
        
        # 2. Вывод диапазона по ID
        users_range = db.select_by_id_range('users777', 1, 3)
        print(f"\n2. Диапазон по ID (1-3): {len(users_range)} записей")
        for u in users_range:
            print(f"   ID: {u['id']}, Name: {u['name']}")
        
        # 3. Удаление диапазона по ID
        deleted = db.delete_by_id_range('users777', 4, 5)
        print(f"\n3. Удалено записей: {deleted}")
        
        # 4. Структура таблицы
        structure = db.describe_table('users777')
        print(f"\n4. Структура таблицы:")
        for col in structure:
            print(f"   {col['Field']} - {col['Type']}")
        
        # 5. Поиск по значению в столбце
        users_named_alex = db.select_where_column('users777', 'name', 'Alex')
        print(f"\n5. Поиск 'Alex': {len(users_named_alex)} записей")
        
        # 6. Удаление таблицы (раскомментировать для удаления)
        # db.drop_table('users777')
        # print("6. Таблица удалена")
        
        # 7. Добавление/удаление колонки
        db.add_column('users777', 'city', 'VARCHAR(100)')
        print("7. Колонка 'city' добавлена")
        db.drop_column('users777', 'city')
        print("7. Колонка 'city' удалена")
        
        # 8. Экспорт/импорт CSV
        count = db.export_to_csv('users777', 'users777.csv')
        print(f"\n8. Экспортировано: {count} записей в users777.csv")
        
        imported = db.import_from_csv('users777', 'users777.csv')
        print(f"8. Импортировано: {imported} записей")
