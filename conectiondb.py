from pgsql_lib import pgsql_class as pg
import pandas as pd



sql = '''CREATE TABLE public.database_olx 
      ( id            character varying(10), 
        name          character varying(100), 
        title         character varying(50),
        price         character varying(1000), 
        siglaUf       character varying(10), 
        urlImage      character varying(100), 
        email         character varying(100),

      )'''
inserir_db(sql)


for i in df.index:
    sql = """
    INSERT into public.database_olx (id,name, title, price, siglaUf, urlImage, email) 
    values('%s','%s','%s','%s','%s','%s','%s');
    """ % (df['id'][i], df['nome'][i], df['title'][i], df['price'][i], df['siglaUf'][i], df['urlImage'][i], df['email'][i])
    inserir_db(sql)
