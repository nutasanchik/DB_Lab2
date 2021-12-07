import controller as con
from psycopg2 import Error
import sys

c = con.Controller()

try:
    command = sys.argv[1]
except IndexError:
    c.v.no_command()
else:
    if command == 'print_table':
        try:
            name = sys.argv[2]
        except IndexError:
            c.v.argument_error()
        else:
            c.print(name)

    elif command == 'delete_record':
        try:
            args = {"name": sys.argv[2], "key": sys.argv[3], "val": sys.argv[4]}
        except IndexError:
            c.v.argument_error()
        else:
            c.delete(args["name"], args["key"], args["val"])

    elif command == 'update_record':
        try:
            args = {"table": sys.argv[2], "key": sys.argv[3]}
            if args["table"] == 'Cinema':
                args["name"], args["id_movie"] = sys.argv[4], sys.argv[5]
            elif args["table"] == 'Movie':
                args["title"], args["description"] = sys.argv[4], sys.argv[5]
            elif args["table"] == 'Showtime':
                args["timing"], args["id_movie"] = sys.argv[4], sys.argv[5]
            elif args["table"] == 'Ticket':
                args["name"], args["date"], args["price"], args["id_showtime"], args["row"], args["place"] = \
                    sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8], sys.argv[9]
            else:
                c.v.wrong_table()
        except IndexError:
            c.v.argument_error()
        else:
            if args["table"] == 'Cinema':
                c.update_cinema(args["key"], args["name"], args["id_movie"])
            elif args["table"] == 'Movie':
                c.update_movie(args["key"], args["title"], args["description"])
            elif args["table"] == 'Showtime':
                c.update_showtime(args["key"], args["timing"], args["id_movie"])
            elif args["table"] == 'Ticket':
                c.update_ticket(args["key"], args["name"], args["date"], args["price"], args["id_showtime"],
                                args["row"], args["place"])

    elif command == 'insert_record':
        try:
            args = {"table": sys.argv[2], "key": sys.argv[3]}
            if args["table"] == 'Cinema':
                args["name"], args["id_movie"] = sys.argv[4], sys.argv[5]
            elif args["table"] == 'Movie':
                args["title"], args["description"] = sys.argv[4], sys.argv[5]
            elif args["table"] == 'Showtime':
                args["timing"], args["id_movie"] = sys.argv[4], sys.argv[5]
            elif args["table"] == 'Ticket':
                args["name"], args["date"], args["price"], args["id_showtime"], args["row"], args["place"] = \
                    sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8], sys.argv[9]
            else:
                c.v.wrong_table()
        except IndexError:
            c.v.argument_error()
        else:
            if args["table"] == 'Cinema':
                c.insert_cinema(args["key"], args["name"], args["id_movie"])
            elif args["table"] == 'Movie':
                c.insert_movie(args["key"], args["title"], args["description"])
            elif args["table"] == 'Showtime':
                c.insert_showtime(args["key"], args["timing"], args["id_movie"])
            elif args["table"] == 'Ticket':
                c.insert_ticket(args["key"], args["name"], args["date"], args["price"], args["id_showtime"],
                                args["row"], args["place"])

    elif command == 'generate_randomly':
        try:
            args = {"name": sys.argv[2], "n": int(sys.argv[3])}
        except (IndexError, Exception):
            print(Exception, IndexError)
        else:
            c.generate(args["name"], args["n"])

    elif command == 'search_records':
        if len(sys.argv) in [6, 9, 12]:
            search_num = c.v.get_search_num()
            try:
                search_num = int(search_num)
            except ValueError:
                c.v.invalid_search_num()
            else:
                if search_num > 0:
                    if len(sys.argv) == 6:
                        args = {"table1_name": sys.argv[2], "table2_name": sys.argv[3],
                                "key1_name": sys.argv[4], "key2_name": sys.argv[5]}
                        c.search_two(args["table1_name"], args["table2_name"], args["key1_name"], args["key2_name"],
                                     c.v.proceed_search(search_num))
                    elif len(sys.argv) == 9:
                        args = {"table1_name": sys.argv[2], "table2_name": sys.argv[3], "table3_name": sys.argv[4],
                                "key1_name": sys.argv[5], "key2_name": sys.argv[6], "key3_name": sys.argv[7],
                                "key13_name": sys.argv[8]}
                        c.search_three(args["table1_name"], args["table2_name"], args["table3_name"],
                                       args["key1_name"], args["key2_name"], args["key3_name"], args["key13_name"],
                                       c.v.proceed_search(search_num))
                    elif len(sys.argv) == 12:
                        args = {"table1_name": sys.argv[2], "table2_name": sys.argv[3], "table3_name": sys.argv[4],
                                "table4_name": sys.argv[5],
                                "key1_name": sys.argv[6], "key2_name": sys.argv[7], "key3_name": sys.argv[8],
                                "key13_name": sys.argv[9], "key4_name": sys.argv[10], "key24_name": sys.argv[11]}
                        c.search_four(args["table1_name"], args["table2_name"], args["table3_name"],
                                      args["table4_name"],
                                      args["key1_name"], args["key2_name"], args["key3_name"], args["key13_name"],
                                      args["key4_name"], args["key24_name"], c.v.proceed_search(search_num))
                else:
                    c.v.invalid_search_num()
        else:
            c.v.argument_error()

    elif command == 'help':
        c.v.print_help()
    else:
        c.v.wrong_command()
