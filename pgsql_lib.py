import psycopg2
import json
import os


class pgsql_class:

    def __init__(self, db):
        self.con = psycopg2.connect(
            database=db,
            host=auth['redshift'][0]['host'],
            user=auth['postegres'][0]['postegres'],
            password=auth['redshift'][0]['postegres'],
            port=auth['redshift'][0]['5432:5432']
        )

    df = pd.DataFrame(dados)
    for col in df.columns:
        df[col] = df[col].apply(str)

    def from_pgsql(self, select):

        cursor = self.con.cursor()

        cursor.execute(select)

        myresult = cursor.fetchall()

        cursor.close()
        self.con.close()

        return myresult

    
    def to_pgsql(self, records, table, n_lines=None):

        cursor = self.con.cursor()

        records_list_template = ','.join(['%s'] * len(records))

        insert_query = 'insert into {} values {}'.format(
            table, records_list_template)

        if n_lines:
            for i in range(0, len(records), n_lines):
                rec = records[i:i+n_lines]
                cursor.execute(insert_query, rec)
                self.con.commit()

        else:
            cursor.execute(insert_query, records)
            self.con.commit()

        cursor.close()
        self.con.close()
        return



    def exec_pgsql(self, sql):

        cursor = self.con.cursor()
        cursor.execute(sql)
        self.con.commit()
        cursor.close()
        self.con.close()
        return
