
from timescale_connector import TimescaleConnection
import os
import dotenv

dotenv.load_dotenv()

def migrate_indx_descriptions():
    db_old = TimescaleConnection(os.getenv("DB_HOST"), os.getenv("DB_PORT"), os.getenv("DB_NAME"), os.getenv("DB_USER"), os.getenv("DB_PASS"))
    db_old.connect()

    print(os.getenv("DB_HOST_NEW"))
    db_new = TimescaleConnection(os.getenv("DB_HOST_NEW"), os.getenv("DB_PORT_NEW"), os.getenv("DB_NAME_NEW"), os.getenv("DB_USER_NEW"), os.getenv("DB_PASS_NEW"))
    db_new.connect()

    query = "SELECT * from indx_descriptions"
    res = db_old.exec(query)
    for item in res: 
        print(item[0], item[1])
        db_new.insert("indx_descriptions",{
            'address': item[0], 
            'description': item[1],
        })

    db_old.close()
    db_new.close()

migrate_indx_descriptions()
