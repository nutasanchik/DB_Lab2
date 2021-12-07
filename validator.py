
class Validator:
    def __init__(self):
        self.error = ''
        self.er_flag = False

    def check_table_name(self, arg: str):
        if arg in ['Cinema', 'Movie', 'Showtime', 'Ticket']:
            return arg
        else:
            self.er_flag = True
            self.error = f'table {arg} does not exist in the database'
            print(self.error)
            return False

    def check_pkey_value(self, arg: str, min_val: int, max_val: int):
        try:
            value = int(arg)
        except ValueError:
            self.er_flag = True
            self.error = f'{arg} is not correct primary key value'
            print(self.error)
            return 0
        else:
            if min_val <= value <= max_val:
                return value
            else:
                self.er_flag = True
                self.error = f'{arg} is not existing primary key value'
                print(self.error)
                return 0

    def check_pk_name(self, table_name: str, key_name: str):
        if table_name == 'Cinema' and key_name == 'id' \
                or table_name == 'Movie' and key_name == 'id' \
                or table_name == 'Showtime' and key_name == 'id' \
                or table_name == 'Ticket' and key_name == 'id':
            return key_name
        else:
            self.er_flag = True
            self.error = f'key {key_name} is not a primary key of table {table_name}'
            print(self.error)
            return False

    def check_pk(self, val, count):
        try:
            value = int(val)
        except ValueError:
            self.er_flag = True
            self.error = f'{val} is not correct primary key value'
            print(self.error)
            return 0
        else:
            if count and not count == (0,):
                return value
            else:
                return 0

    def check_key_names(self, table_name: str, key: str):
        if table_name == 'Cinema' and key in ['id', 'name', 'id_movie']:
            return True
        elif table_name == 'Movie' and key in ['id', 'title', 'description']:
            return True
        elif table_name == 'Showtime' and key in ['id', 'timing', 'id_movie']:
            return True
        elif table_name == 'Ticket' and key in ['id', 'name', 'date', 'price', 'id_showtime', 'row', 'place']:
            return True
        else:
            self.er_flag = True
            self.error = f'{key} is not correct name for {table_name} table'
            print(self.error)
            return False

    def check_possible_keys(self, table_name: str, key: str, val):
        if table_name == 'Cinema':
            if key in ['id', 'id_movie']:
                try:
                    value = int(val)
                except ValueError:
                    self.er_flag = True
                    self.error = f'{val} is not correct key value'
                    print(self.error)
                    return False
                else:
                    return True
            elif key in ['name']:
                return True

            else:
                self.er_flag = True
                self.error = f'{key} is not correct name for Cinema table'
                print(self.error)
                return False
        elif table_name == 'Movie':
            if key in ['id']:
                try:
                    value = int(val)
                except ValueError:
                    self.er_flag = True
                    self.error = f'{val} is not correct key value'
                    print(self.error)
                    return False
                else:
                    return True
            elif key == ['title', 'description']:
                return True

            else:
                self.er_flag = True
                self.error = f'{key} is not correct name for Movie table'
                print(self.error)
                return False
        elif table_name == 'Showtime':
            if key in ['id', 'id_movie']:
                try:
                    value = int(val)
                except ValueError:
                    self.er_flag = True
                    self.error = f'{val} is not correct key value'
                    print(self.error)
                    return False
                else:
                    return True

            elif key == ['timing']:
                try:
                    value = float(val)
                except ValueError:
                    self.er_flag = True
                    self.error = f'{val} is not correct price value'
                    print(self.error)
                    return False
                else:
                    return True
            else:
                self.er_flag = True
                self.error = f'{key} is not correct name for Showtime table'
                print(self.error)
                return False
        elif table_name == 'Ticket':
            if key == ['id', 'id_showtime']:
                try:
                    value = int(val)
                except ValueError:
                    self.er_flag = True
                    self.error = f'{val} is not correct key value'
                    print(self.error)
                    return False
                else:
                    return True
            elif key in ['name', 'date']:
                return True
            elif key == ['price', 'row', 'place']:
                try:
                    value = float(val)
                except ValueError:
                    self.er_flag = True
                    self.error = f'{val} is not correct price value'
                    print(self.error)
                    return False
                else:
                    return True

