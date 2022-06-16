import cx_Oracle
import csv
#import config as cfg
from collections import namedtuple
import time

lib_dir="/Users/bhayes/oracle_libs"
#lib_dir = "/opt/oracle/instantclient_21_4"
cx_Oracle.init_oracle_client(lib_dir=lib_dir)

csv_file = "./FakeNameGenerator.csv"

# connection = cx_Oracle.connect(
#    cfg.username,
#    cfg.password,
#    cfg.dsn,
#    encoding=cfg.encoding
# )

connection = cx_Oracle.connect(
    user='C##MYUSER',
    password='mypassword',
    dsn='localhost/ORCLCDB',
    #port=1512,
    encoding='UTF-8'
)

with open('ny_zips.csv', mode='r') as input:
    reader = csv.reader(input)
    next(reader)
    zip_county_dict = {rows[0]: rows[2] for rows in reader}

with open(csv_file, 'r', encoding='utf-8-sig') as f:
    f_csv = csv.reader(f)
    headings = next(f_csv)
    Row = namedtuple('Row', headings)

    for r in f_csv:
        row = Row(*r)
        GivenName = row.GivenName
        Surname = row.Surname
        MiddleInitial = row.MiddleInitial
        StreetAddress = row.StreetAddress
        City = row.City
        State = row.State
        ZipCode = row.ZipCode
        County = zip_county_dict.get(row.ZipCode)
        if County is None:
            County = "No County"
        EmailAddress = row.EmailAddress
        Birthday = row.Birthday
        NationalID = row.NationalID
        Occupation = row.Occupation

        sql = (
            'insert into APPLICANTS(GIVENNAME, SURNAME, MIDDLEINITIAL, STREETADDRESS, CITY, STATE, ZIPCODE, COUNTY, EMAILADDRESS, BIRTHDAY, NATIONALID, OCCUPATION)'
            'values(:GivenName, :Surname, :MiddleInitial, :StreetAddress, :City, :State, :ZipCode, :County, :EmailAddress, :Birthday, :NationalID, :Occupation)')

        try:
            with cx_Oracle.connect(
                    user='C##MYUSER',
                    password='mypassword',
                    dsn='localhost/ORCLCDB',
                    # port=1512,
                    encoding='UTF-8'
            ) as connection:

                with connection.cursor() as cursor:
                    cursor.execute(sql, [GivenName, Surname, MiddleInitial, StreetAddress, City, State, ZipCode, County,
                                         EmailAddress, Birthday, NationalID, Occupation])
                    connection.commit()
                    print(GivenName + " " + MiddleInitial + " " + Surname + " was added")

        except cx_Oracle.Error as error:
            print('Error occuured:')
            print(error)

        time.sleep(5)
