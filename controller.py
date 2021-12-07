import psycopg2
from psycopg2 import Error
import model
import view
import time


class Controller:
    def __init__(self):
        self.v = view.View()
        self.m = model.Model()

    def print(self, table_name):
        t_name = self.v.valid.check_table_name(table_name)
        if t_name:
            if t_name == 'Cinema':
                self.v.print_cinema(self.m.print_cinema())
            elif t_name == 'Movie':
                self.v.print_movie(self.m.print_movie())
            elif t_name == 'Showtime':
                self.v.print_showtime(self.m.print_showtime())
            elif t_name == 'Ticket':
                self.v.print_ticket(self.m.print_ticket())

    def delete(self, table_name, key_name, value):
        t_name = self.v.valid.check_table_name(table_name)
        k_name = self.v.valid.check_pk_name(table_name, key_name)
        if t_name and k_name:
            count = self.m.find(t_name, k_name, value)
            k_val = self.v.valid.check_pk(value, count)
            if k_val:
                if t_name == 'Showtime' or t_name == 'Cinema':
                    count_p = self.m.find('Movie', k_name, value)[0]
                    if count_p:
                        self.v.cannot_delete()
                    else:
                        try:
                            self.m.delete_data(table_name, key_name, k_val)
                        except (Exception, Error) as _ex:
                            self.v.sql_error(_ex)
                elif t_name == 'Showtime':
                    count_s = self.m.find('Ticket', k_name, value)[0]
                    if count_s:
                        self.v.cannot_delete()
                    else:
                        try:
                            self.m.delete_data(table_name, key_name, k_val)
                        except (Exception, Error) as _ex:
                            self.v.sql_error(_ex)
                else:
                    try:
                        self.m.delete_data(table_name, key_name, k_val)
                    except (Exception, Error) as _ex:
                        self.v.sql_error(_ex)
            else:
                self.v.deletion_error()

    def update_cinema(self, key: str, name: str, id_movie: int):
        if self.v.valid.check_possible_keys('Cinema', 'id', key):
            count_c = self.m.find('Cinema', 'id', int(key))
            c_val = self.v.valid.check_pk(key, count_c)
        if self.v.valid.check_possible_keys('Movie', 'id', id_movie):
            count_m = self.m.find('Movie', 'id', int(id_movie))
            m_val = self.v.valid.check_pk(id_movie, count_m)

        if c_val and name and m_val:
            try:
                self.m.update_data_cinema(c_val, name, m_val)
            except (Exception, Error) as _ex:
                self.v.sql_error(_ex)
        else:
            self.v.updation_error()

    def update_movie(self, key: str, title: str, description: str):
        if self.v.valid.check_possible_keys('Movie', 'id', key):
            count_m = self.m.find('Movie', 'id', int(key))
            m_val = self.v.valid.check_pk(key, count_m)

        if m_val and title and description:
            try:
                self.m.update_data_movie(m_val, title, description)
            except (Exception, Error) as _ex:
                self.v.sql_error(_ex)
        else:
            self.v.updation_error()

    def update_showtime(self, key: str, timing: int, id_movie: int):
        if self.v.valid.check_possible_keys('Showtime', 'id', key):
            count_s = self.m.find('Showtime', 'id', int(key))
            s_val = self.v.valid.check_pk(key, count_s)
        if self.v.valid.check_possible_keys('Movie', 'id', id_movie):
            count_m = self.m.find('Movie', 'id', int(id_movie))
            m_val = self.v.valid.check_pk(id_movie, count_m)

        if s_val and timing and m_val:
            try:
                self.m.update_data_showtime(s_val, timing, m_val)
            except (Exception, Error) as _ex:
                self.v.sql_error(_ex)
        else:
            self.v.updation_error()

    def update_ticket(self, key: str, name: str, date: str, price: int, id_showtime: int, row: int, place: int):
        if self.v.valid.check_possible_keys('Ticket', 'id', key):
            count_t = self.m.find('Ticket', 'id', int(key))
            t_val = self.v.valid.check_pk(key, count_t)
        if self.v.valid.check_possible_keys('Showtime', 'id', id_showtime):
            count_s = self.m.find('Showtime', 'id', int(id_showtime))
            s_val = self.v.valid.check_pk(id_showtime, count_s)

        if key and name and date and price and s_val and row and place:
            try:
                self.m.update_data_ticket(int(key), name, date, price, s_val, row, place)
            except (Exception, Error) as _ex:
                self.v.sql_error(_ex)
        else:
            self.v.updation_error()

    def insert_cinema(self, key: str, name: str, id_movie: int):
        if self.v.valid.check_possible_keys('Cinema', 'id', key):
            count_c = self.m.find('Cinema', 'id', int(key))
        if self.v.valid.check_possible_keys('Movie', 'id', id_movie):
            count_m = self.m.find('Movie', 'id', int(id_movie))
            m_val = self.v.valid.check_pk(id_movie, count_m)

        if (not count_c or count_c == (0,)) and name and m_val:
            try:
                self.m.insert_data_cinema(int(key), name, m_val)
            except (Exception, Error) as _ex:
                self.v.sql_error(_ex)
        else:
            self.v.insertion_error()

    def insert_movie(self, key: str, title: str, description: str):
        if self.v.valid.check_possible_keys('Movie', 'id', key):
            count_m = self.m.find('Movie', 'id', int(key))

        if (not count_m or count_m == (0,)) and title and description:
            try:
                self.m.insert_data_movie(int(key), title, description)
            except (Exception, Error) as _ex:
                self.v.sql_error(_ex)
        else:
            self.v.insertion_error()

    def insert_showtime(self, key: str, timing: int, id_movie: int):
        if self.v.valid.check_possible_keys('Showtime', 'id', key):
            count_s = self.m.find('Showtime', 'id', int(key))
        if self.v.valid.check_possible_keys('Movie', 'id', id_movie):
            count_m = self.m.find('Movie', 'id', int(id_movie))
            m_val = self.v.valid.check_pk(id_movie, count_m)

        if (not count_s or count_s == (0,)) and timing and m_val:
            try:
                self.m.insert_data_showtime(int(key), timing, m_val)
            except (Exception, Error) as _ex:
                self.v.sql_error(_ex)
        else:
            self.v.insertion_error()

    def insert_ticket(self, key: str, name: str, date: str, price: int, id_showtime: int, row: int, place: int):
        if self.v.valid.check_possible_keys('Ticket', 'id', key):
            count_t = self.m.find('Ticket', 'id', int(key))
        if self.v.valid.check_possible_keys('Showtime', 'id', id_showtime):
            count_s = self.m.find('Showtime', 'id', int(id_showtime))
            s_val = self.v.valid.check_pk(id_showtime, count_s)

        try:
            self.m.insert_data_ticket(int(key), name, date, price, s_val, row, place)
        except (Exception, Error) as _ex:
            self.v.sql_error(_ex)

    def generate(self, table_name: str, n: int):
        t_name = self.v.valid.check_table_name(table_name)
        if t_name:
            if t_name == 'Cinema':
                self.m.cinema_data_generator(n)
            elif t_name == 'Movie':
                self.m.movie_data_generator(n)
            elif t_name == 'Showtime':
                self.m.showtime_data_generator(n)
            elif t_name == 'Ticket':
                self.m.ticket_data_generator(n)

    def search_two(self, table1_name: str, table2_name: str, table1_key: str, table2_key: str, search: str):
        t1_n = self.v.valid.check_table_name(table1_name)
        t2_n = self.v.valid.check_table_name(table2_name)
        if t1_n and self.v.valid.check_key_names(t1_n, table1_key) and t2_n \
                and self.v.valid.check_key_names(t2_n, table2_key):
            start_time = time.time()
            result = self.m.search_data_two_tables(table1_name, table2_name, table1_key, table2_key,
                                                   search)
            self.v.print_time(start_time)

            self.v.print_search(result)

    def search_three(self, table1_name: str, table2_name: str, table3_name: str,
                     table1_key: str, table2_key: str, table3_key: str, table13_key: str,
                     search: str):
        t1_n = self.v.valid.check_table_name(table1_name)
        t2_n = self.v.valid.check_table_name(table2_name)
        t3_n = self.v.valid.check_table_name(table3_name)
        if t1_n and self.v.valid.check_key_names(t1_n, table1_key) and self.v.valid.check_key_names(t1_n, table13_key) \
                and t2_n and self.v.valid.check_key_names(t2_n, table2_key) \
                and t3_n and self.v.valid.check_key_names(t3_n, table3_key) \
                and self.v.valid.check_key_names(t3_n, table13_key):
            start_time = time.time()
            result = self.m.search_data_three_tables(table1_name, table2_name, table3_name,
                                                     table1_key, table2_key, table3_key, table13_key,
                                                     search)
            self.v.print_time(start_time)
            self.v.print_search(result)

    def search_four(self, table1_name: str, table2_name: str, table3_name: str, table4_name: str,
                    table1_key: str, table2_key: str, table3_key: str, table13_key: str,
                    table4_key: str, table24_key: str,
                    search: str):
        t1_n = self.v.valid.check_table_name(table1_name)
        t2_n = self.v.valid.check_table_name(table2_name)
        t3_n = self.v.valid.check_table_name(table3_name)
        t4_n = self.v.valid.check_table_name(table2_name)
        if t1_n and self.v.valid.check_key_names(t1_n, table1_key) and self.v.valid.check_key_names(t1_n, table13_key) \
                and t2_n and self.v.valid.check_key_names(t2_n, table2_key) \
                and self.v.valid.check_key_names(t2_n, table24_key) \
                and t3_n and self.v.valid.check_key_names(t3_n, table3_key) \
                and self.v.valid.check_key_names(t3_n, table13_key) \
                and t4_n and self.v.valid.check_key_names(t4_n, table4_key) \
                and self.v.valid.check_key_names(t4_n, table24_key):

            start_time = time.time()
            result = self.m.search_data_all_tables(table1_name, table2_name, table3_name, table4_name,
                                                   table1_key, table2_key, table3_key, table13_key,
                                                   table4_key, table24_key,
                                                   search)
            self.v.print_time(start_time)
            self.v.print_search(result)
