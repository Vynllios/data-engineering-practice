import psycopg2
import os

def connect():
    host = 'localhost'
    database = 'test'
    user = 'postgres'
    pas = '*****'
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    try:
        cur = conn.cursor()

        # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)


    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    def create_table():
        table_commands=('''DROP TABLE IF EXISTS transactions
            ''',
            '''DROP TABLE IF EXISTS accounts
            ''',
            '''DROP TABLE IF EXISTS products
            ''',
            ''' CREATE TABLE products (
            product_id INT NOT NULL,
            product_code INT NOT NULL,
            product_description varchar,
            PRIMARY KEY (product_id,product_code)
            )
            ''',
            ''' CREATE TABLE accounts (
            customer_id INT NOT NULL PRIMARY KEY,
            first_name VARCHAR NOT NULL,
            last_name VARCHAR NOT NULL,
            address_1 VARCHAR NOT NULL,
            address_2 VARCHAR,
            city VARCHAR NOT NULL,
            state VARCHAR NOT NULL,
            zip_code INT NOT NULL,
            join_date DATE NOT NULL
            )
            ''',
            '''CREATE TABLE transactions (
            transaction_id  VARCHAR(27) NOT NULL PRIMARY KEY,
            transaction_date DATE NOT NULL,
            product_id INT NOT NULL,
            product_code INT NOT NULL,
            product_description varchar,
            quantity INT NOT NULL,
            account_id INT NOT NULL,
            FOREIGN KEY (product_id,product_code)
                REFERENCES products (product_id,product_code)
                ON UPDATE CASCADE ON DELETE CASCADE,
            FOREIGN KEY (account_id)
                REFERENCES accounts (customer_id)
                ON UPDATE CASCADE ON DELETE CASCADE
                )
            '''
        )
        for command in table_commands:
            cur.execute(command)
        conn.commit()
    create_table()
    cwd=os.getcwd()
    file1=os.path.join(cwd,'data','accounts.csv')
    file2=os.path.join(cwd,'data','products.csv')
    file3=os.path.join(cwd,'data','transactions.csv')
    def import_csv():
        sql1='''COPY products(product_id,product_code,product_description) 
        FROM %s
        DELIMITER ','
        CSV HEADER;
        '''
        cur.execute(sql1,[file2])
        sql2='''COPY accounts(customer_id, first_name,last_name,address_1,address_2,city,state,zip_code,join_date)
        FROM %s
        DELIMITER ','
        CSV HEADER;
        '''
        cur.execute(sql2,[file1])
        sql3='''COPY transactions(transaction_id,transaction_date,product_id,product_code,product_description,quantity,account_id)
        FROM %s
        DELIMITER ','
        CSV HEADER
        '''
        cur.execute(sql3, [file3])
        cur.close()
        conn.commit()
    import_csv()



    if conn is not None:
        conn.close()

if __name__ == '__main__':
    connect()

