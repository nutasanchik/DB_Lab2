import psycopg2 as ps


class Model:
    def __init__(self):
        self.conn = None
        try:
            self.conn = ps.connect(
                dbname="Cinema_1",
                user='postgres',
                password="12345",
                host='127.0.0.1',
                port="5432",
            )
        except(Exception, ps.DatabaseError) as error:
            print("[INFO] Error while working with Postgresql", error)

    def request(self, req: str):
        try:
            cursor = self.conn.cursor()
            print(req)
            cursor.execute(req)
            self.conn.commit()
            return True
        except(Exception, ps.DatabaseError, ps.ProgrammingError) as error:
            print(error)
            self.conn.rollback()
            return False

    def get(self, req: str):
        try:
            cursor = self.conn.cursor()
            print(req)
            cursor.execute(req)
            self.conn.commit()
            return cursor.fetchall()
        except(Exception, ps.DatabaseError, ps.ProgrammingError) as error:
            print(error)
            self.conn.rollback()
            return False

    def get_el(self, req: str):
        try:
            cursor = self.conn.cursor()
            print(req)
            cursor.execute(req)
            self.conn.commit()
            return cursor.fetchone()
        except(Exception, ps.DatabaseError, ps.ProgrammingError) as error:
            print(error)
            self.conn.rollback()
            return False

    def count(self, table_name: str):
        return self.get_el(f"select count(*) from public.\"{table_name}\"")

    def find(self, table_name: str, key_name: str, key_value: int):
        return self.get_el(f"select count(*) from public.\"{table_name}\" where {key_name}={key_value}")

    def max(self, table_name: str, key_name: str):
        return self.get_el(f"select max({key_name}) from public.\"{table_name}\"")

    def min(self, table_name: str, key_name: str):
        return self.get_el(f"select min({key_name}) from public.\"{table_name}\"")

    def print_cinema(self) -> None:
        return self.get(f"SELECT * FROM public.\"Cinema\"")

    def print_movie(self) -> None:
        return self.get(f"SELECT * FROM public.\"Movie\"")

    def print_showtime(self) -> None:
        return self.get(f"SELECT * FROM public.\"Showtime\"")

    def print_ticket(self) -> None:
        return self.get(f"SELECT * FROM public.\"Ticket\"")

    def delete_data(self, table_name: str, key_name: str, key_value) -> None:
        self.request(f"DELETE FROM public.\"{table_name}\" WHERE {key_name}={key_value};")

    def update_data_cinema(self, key_value: int, name: str, id_movie: int) -> None:
        self.request(f"UPDATE public.\"Cinema\" SET name=\'{name}\', id_movie=\'{id_movie}\' WHERE id={key_value};")

    def update_data_movie(self, key_value: int, title: str, description: str) -> None:
        self.request(f"UPDATE public.\"Movie\" SET title=\'{title}\', description=\'{description}\' "
                     f"WHERE id={key_value};")

    def update_data_showtime(self, key_value: int, timing: int, id_movie: int) -> None:
        self.request(f"UPDATE public.\"Showtime\" SET timing=\'{timing}\', id_movie=\'{id_movie}\' WHERE id={key_value};")

    def update_data_ticket(self, key_value: int, name: str, date: str, price: int, id_showtime: int, row: int,
                           place: int) -> None:
        self.request(f"UPDATE public.\"Ticket\" SET name=\'{name}\', date=\'{date}\', price=\'{price}\', "
                     f"id_showtime=\'{id_showtime}\', row=\'{row}\', place=\'{place}\' WHERE id={key_value};")

    def insert_data_cinema(self, key_value: int, name: str, id_movie: int) -> None:
        self.request(f"insert into public.\"Cinema\" (id, name, id_movie) "
                     f"VALUES ({key_value}, \'{name}\', \'{id_movie}\');")

    def insert_data_movie(self, key_value: int, title: str, description: str) -> None:
        self.request(f"insert into public.\"Movie\" (id, title, description) "
                     f"VALUES ({key_value}, \'{title}\', \'{description}\');")

    def insert_data_showtime(self, key_value: int, timing: int, id_movie: int) -> None:
        self.request(f"insert into public.\"Showtime\" (id, timing, id_movie) "
                     f"VALUES ({key_value}, \'{timing}\', \'{id_movie}\');")

    def insert_data_ticket(self, key_value: int, name: str, date: str, price: int, id_showtime: int, row: int,
                           place: int) -> None:
        self.request(f"insert into public.\"Ticket\" (id, name, date, price, id_showtime, row, place) "
                     f"VALUES ({key_value}, \'{name}\', \'{date}\', \'{price}\', \'{id_showtime}\', \'{row}\', \'{place}\');")

    def cinema_data_generator(self, times: int) -> None:
        for i in range(times):
            self.request("insert into public.\"Cinema\""
                         "select (SELECT MAX(id)+1 FROM public.\"Cinema\"), "
                         "array_to_string(ARRAY(SELECT chr((97 + round(random() * 25)) :: integer) \
                         FROM generate_series(1, FLOOR(RANDOM()*(10-4)+4):: integer)), ''), "
                         "(SELECT id FROM public.\"Movie\" LIMIT 1 OFFSET "
                         "(round(random() *((SELECT COUNT(id) FROM public.\"Movie\")-1))));")

    def movie_data_generator(self, times: int) -> None:
        for i in range(times):
            self.request("insert into public.\"Movie\" "
                         "select (SELECT (MAX(id)+1) FROM public.\"Movie\"), "
                         "array_to_string(ARRAY(SELECT chr((97 + round(random() * 25)) :: integer) \
                         FROM generate_series(1, FLOOR(RANDOM()*(10-4)+4):: integer)), ''), "
                         "array_to_string(ARRAY(SELECT chr((97 + round(random() * 25)) :: integer) \
                         FROM generate_series(1, FLOOR(RANDOM()*(10-4)+4):: integer)), ''); ")

    def showtime_data_generator(self, times: int) -> None:
        for i in range(times):
            self.request("insert into public.\"Showtime\" "
                         "select (SELECT MAX(id)+1 FROM public.\"Showtime\"), "
                         "FLOOR(RANDOM()*(100000-1)+1),"
                         "(SELECT id FROM public.\"Movie\" LIMIT 1 OFFSET "
                         "(round(random() *((SELECT COUNT(id) FROM public.\"Movie\")-1))));")

    def ticket_data_generator(self, times: int) -> None:
        for i in range(times):
            self.request("insert into public.\"Ticket\" "
                         "select (SELECT MAX(id)+1 FROM public.\"Ticket\"), "
                         "array_to_string(ARRAY(SELECT chr((97 + round(random() * 25)) :: integer) \
                         FROM generate_series(1, FLOOR(RANDOM()*(10-4)+4):: integer)), ''), "
                         "array_to_string(ARRAY(SELECT chr((150 + round(random() * 25)) :: integer) \
                         FROM generate_series(1, FLOOR(RANDOM()*(10-4)+4):: integer)), ''), "
                         "FLOOR(RANDOM()*(100000-1)+1),"
                         "(SELECT id FROM public.\"Showtime\" LIMIT 1 OFFSET "
                         "(round(random() *((SELECT COUNT(id) FROM public.\"Showtime\")-1)))), "
                         "FLOOR(RANDOM()*(100000-1)+1),"
                         "FLOOR(RANDOM()*(100000-1)+1);")

    def search_data_two_tables(self, table1_name: str, table2_name: str, table1_key, table2_key,
                               search: str):
        return self.get(f"select * from public.\"{table1_name}\" as one inner join public.\"{table2_name}\" as two "
                        f"on one.\"{table1_key}\"=two.\"{table2_key}\" "
                        f"where {search}")

    def search_data_three_tables(self, table1_name: str, table2_name: str, table3_name: str,
                                 table1_key, table2_key, table3_key, table13_key,
                                 search: str):
        return self.get(f"select * from public.\"{table1_name}\" as one inner join public.\"{table2_name}\" as two "
                        f"on one.\"{table1_key}\"=two.\"{table2_key}\" inner join public.\"{table3_name}\" as three "
                        f"on three.\"{table3_key}\"=one.\"{table13_key}\""
                        f"where {search}")

    def search_data_all_tables(self, table1_name: str, table2_name: str, table3_name: str, table4_name: str,
                               table1_key, table2_key, table3_key, table13_key,
                               table4_key, table24_key,
                               search: str):
        return self.get(f"select * from public.\"{table1_name}\" as one inner join public.\"{table2_name}\" as two "
                        f"on one.\"{table1_key}\"=two.\"{table2_key}\" inner join public.\"{table3_name}\" as three "
                        f"on three.\"{table3_key}\"=one.\"{table13_key}\" inner join public.\"{table4_name}\" as four "
                        f"on four.\"{table4_key}\"=two.\"{table24_key}\""
                        f"where {search}")
