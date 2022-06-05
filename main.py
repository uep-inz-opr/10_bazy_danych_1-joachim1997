import csv, sqlite3

def sum_duration(connection):
    cursor = connection.cursor()
    cursor.execute('''SELECT sum(duration) from polaczenia_duze;''')
    result = cursor.fetchone()[0]
    return result
    
if __name__ == '__main__':
    sqlite_connection = sqlite3.connect(":memory:", detect_types = sqlite3.PARSE_COLNAMES|sqlite3.PARSE_DECLTYPES) # :memory: - temporary database in RAM
    cursor = sqlite_connection.cursor()

    cursor.execute('''CREATE TABLE polaczenia_duze (
                    from_subscriber data_type INTEGER, 
                    to_subscriber data_type INTEGER, 
                    datetime data_type timestamp, 
                    duration data_type INTEGER , 
                    celltower data_type INTEGER);''')

    with open('polaczenia_duze.csv','r') as file: 
        csvreader = csv.reader(file, delimiter = ";")
        next(csvreader, None)
        for polaczenie in csvreader:
            cursor.execute("INSERT INTO polaczenia_duze (from_subscriber, to_subscriber, datetime, duration , celltower) VALUES (?, ?, ?, ?, ?);", polaczenie)
        sqlite_connection.commit()

    print(sum_duration(sqlite_connection))
