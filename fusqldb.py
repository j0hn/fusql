# This program is free software. It comes without any warranty, to
# the extent permitted by applicable law. You can redistribute it
# and/or modify it under the terms of the Do What The Fuck You Want
# To Public License, Version 2, as published by Sam Hocevar. See
# http://sam.zoy.org/wtfpl/COPYING for more details.

import time
import XRecord

import common
import fusqlogger

class FusqlDb(object):
    @fusqlogger.log
    def __init__(self, engine, database):
        '''Main api to control the database management'''
        self.db = XRecord.connect(engine, name=database)

    @fusqlogger.log
    def get_cant_rows(self, table_name):
        '''Returns how many rows has a table'''
        rows = self.db.XArray(table_name)
        return len(rows)

    @fusqlogger.log
    def get_elements_by_field(self, field, table):
        '''Returns an specific field of a table'''

        result = self.db.XArray(table)
        return [getattr(x, field) for x in result]

    @fusqlogger.log
    def get_tables(self):
        '''Returns a list with the names of
           the database tables'''
        result = [x.encode("ascii") for x in self.db.Tables if x != "sqlite_sequence"]
        return result

    @fusqlogger.log
    def get_table_structure(self, table):
        '''Returns a list of tuples (name, type) with the
           table columns name and type'''

        # TODO: Magic to guess file mimetype if it's a binary file

        schema = self.db.XSchema(table)
        result = [(x["COLUMN_NAME"].encode("ascii").lower(), \
                   common.DB_TYPE_TRANSLATOR[x["COLUMN_TYPE"].encode("ascii")]) \
                   for x in schema.columns()]

        # response = self.execute_sql(sql, False)

        # result = []
        # for element in response:
        #     element_name = element[1].encode("ascii")
        #     if element_name in common.FILE_SPECIAL_CASES.keys():
        #         element_type = common.FILE_SPECIAL_CASES[element_name]
        #     else:
        #         element_type = common.DB_TYPE_TRANSLATOR[element[2].encode("ascii")]
        #     if element_name == "start": # I can't name a column index,
        #                                 # so I handle it here
        #         element_name = "index"
        #     result.append((element_name, element_type))

        return result

    @fusqlogger.log
    def get_element_ini_data(self, table_name, element_id):
        '''Returns ini formated string with all the
           table fields and data'''

        result = ""

        data = self.get_element_by_id(table_name, element_id)
        structure = self.get_table_structure(table_name)

        index = 0
        for d in data:
            result += structure[index][0] + " = "
            result += str(d) + "\n"
            index += 1

        result = result.encode("ascii")

        return result

    @fusqlogger.log
    def get_element_data(self, table_name, element_column, element_id, use_cache=True):
        '''Returns the data of a cell'''

        result = ""
        # if element_column == "index":
        #     element_column = "start"

        # sql = "SELECT %s FROM '%s' WHERE id = %d" % \
        #       (element_column, table_name, element_id)
        # response = self.execute_sql(sql, False, useCache=use_cache)

        # response = response[0][0]
        # if response is not None:
        #     if type(response) == buffer:
        #         for b in response:
        #             result += b
        #     else:
        #         result = str(response)

        elements = self.db.XArray(table_name)
        for elem in elements:
            if elem.id == element_id:
                result = getattr(elem, element_column)

        return result

    #@fusqlogger.log
    def set_element_data(self, table_name, column_name, element_id, value):
        '''Modifies a table field'''

        if column_name == "index":
            column_name = "start"

        sql = "UPDATE '%s' SET '%s' = '%s' WHERE id = %d" \
              % (table_name, column_name, value, element_id)
        self.execute_sql(sql, dump=False)

    @fusqlogger.log
    def create_table(self, table_name):
        '''Creates a table with an id column'''

        sql = "CREATE TABLE '%s' " % table_name
        sql += "('id' INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL)"
        self.execute_sql(sql)

    @fusqlogger.log
    def create_row(self, table_name, element_id):
        '''Creates a row in a table with an id'''

        sql = "INSERT INTO '%s' (id) VALUES (%d)" % (table_name, element_id)
        self.execute_sql(sql)

    @fusqlogger.log
    def create_column(self, table_name, column_name, column_type):
        '''Creates a column in a table'''

        sql = "ALTER TABLE '%s' ADD COLUMN '%s' %s" % \
              (table_name, column_name, column_type)
        self.execute_sql(sql)

    @fusqlogger.log
    def delete_table(self, table_name):
        '''Removes a table from the database'''

        sql = "DROP TABLE '%s'" % table_name
        self.execute_sql(sql)

    @fusqlogger.log
    def rename_table(self, table_from, table_to):
        '''Renames a table'''

        sql = "ALTER TABLE '%s' RENAME TO '%s'" % (table_from, table_to)
        self.execute_sql(sql)

    @fusqlogger.log
    def create_table_element(self, table_name, element_id):
        '''Creates a table element'''

        sql = "INSERT INTO '%s' (id) VALUES (%d)" % (table_name, element_id)
        self.execute_sql(sql)

    @fusqlogger.log
    def delete_table_element(self, table_name, element_id):
        '''Removes an element of a table'''

        sql = "DELETE FROM '%s' WHERE id = %d" % (table_name, element_id)
        self.execute_sql(sql)


