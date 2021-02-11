import logging
import psycopg2
import json


class Subscriber:
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

    def fetch_next(self):
        subscriber_name = self.config.get('subscription_name')
        self.cursor.execute(f"SELECT * FROM event_subscriptions WHERE subscription_name = %s", (subscriber_name,))
        subscriber = self.cursor.fetchone()
        if not subscriber:
            self.cursor.execute(f"INSERT INTO event_subscriptions (subscription_name) VALUES (%s)", (subscriber_name,))
            self.conn.commit()

        self.cursor.execute(f"SELECT data FROM events "
                            f"INNER JOIN event_subscriptions e ON e.subscription_name = %s "
                            f"WHERE sequence_id > e.latest_sequence_id "
                            f"ORDER BY latest_sequence_id "
                            f"LIMIT 1", (subscriber_name,))
        record = self.cursor.fetchone()
        if not record:
            return None
        return json.loads(record[0])

    def commit(self):
        subscriber_name = self.config.get('subscription_name')
        self.cursor.execute(f"SELECT * FROM event_subscriptions WHERE subscription_name = %s", (subscriber_name,))
        subscriber = self.cursor.fetchone()
        if not subscriber:
            self.cursor.execute(f"INSERT INTO event_subscriptions (subscription_name) VALUES (%s)", (subscriber_name,))
            self.conn.commit()
            return

        self.cursor.execute(f"UPDATE event_subscriptions "
                            f"SET latest_sequence_id = latest_sequence_id + 1 "
                            f"WHERE subscription_name = %s", (subscriber_name,))
        self.conn.commit()
