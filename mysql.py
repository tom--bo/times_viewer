import datetime
from sqlalchemy import create_engine
from sqlalchemy.sql import text

class MySQL:
    def __init__(self, test):
        self.engine = create_engine(
            "mysql://root:root@localhost/slack?charset=utf8",
            encoding='utf-8',
            echo=True
        )

    def get_task_list(self):
        conn = self.engine.connect()
        s = text("SELECT tasks.id, users.name, tasks.name, begin, finish, tasks.created_at FROM tasks INNER JOIN users ON users.id = tasks.uid WHERE tasks.finish IS NOT NULL ORDER BY id ASC")
        tasklist = conn.execute(s).fetchall()

#         for row in tasklist:
#             print(row)
#             print("Name:" + row['name'] + "  begin:" + str(row['begin']) + "  finish:" + str(row['finish']))
        return tasklist

