import logging
import psycopg2
import json


class Publisher:
    def __init__(self, config):
        self.logger = logging.getLogger(__name__)
        self.logger.info(f'Initializing EventBusClient...')
        self.config = config

        self.conn, self.cursor = self.__open__()

    def __open__(self):
        conn = psycopg2.connect(f"dbname='{self.config.get('db').get('name')}' "
                                f"user='{self.config.get('db').get('username')}' "
                                f"host='{self.config.get('db').get('host')}' "
                                f"port='{self.config.get('db').get('port')}' "
                                f"password='{self.config.get('db').get('password')}'")
        cursor = conn.cursor()
        return conn, cursor

    def publish(self, event):
        self.logger.info(f'Publishing new event: [{str(event)}]')
        self.cursor.execute(f"INSERT INTO events (data) VALUES (%s)", (json.dumps(event),))
        self.conn.commit()
