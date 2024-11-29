
import random
import time
from datetime import datetime

import psycopg2
import simplejson as json
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer

from main import delivery_report

conf = {
    'bootstrap.servers': 'localhost:9092',
}

consumer = Consumer(conf | {
    'group.id': 'voting-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

producer = SerializingProducer(conf)

if __name__ == "__main__":
    try:
        # Establish PostgreSQL connection
        conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
        cur = conn.cursor()

        # Fetch candidates from database
        candidates_query = cur.execute("""
            SELECT row_to_json(t)
            FROM (
                SELECT * FROM candidates
            ) t;
        """)
        candidates = cur.fetchall()
        candidates = [candidate[0] for candidate in candidates]

        if len(candidates) == 0:
            raise Exception("No candidates found in the database")
        else:
            print(f"Candidates loaded: {len(candidates)} candidates found")

        consumer.subscribe(['voters_topic'])
        try:
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                elif msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"Kafka error: {msg.error()}")
                        break
                else:
                    # Parse the incoming message (voter data)
                    voter = json.loads(msg.value().decode('utf-8'))
                    chosen_candidate = random.choice(candidates)

                    # Prepare the vote data
                    vote = {
                        **voter,  # voter data from Kafka
                        **chosen_candidate,  # chosen candidate data
                        "voting_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        "vote": 1
                    }

                    try:
                        print(f"User {vote['voter_id']} is voting for candidate: {vote['candidate_id']}")

                        # Insert the vote into the PostgreSQL database
                        cur.execute("""
                            INSERT INTO votes (voter_id, candidate_id, voting_time)
                            VALUES (%s, %s, %s)
                        """, (vote['voter_id'], vote['candidate_id'], vote['voting_time']))
                        conn.commit()

                        # Send the vote data to Kafka
                        producer.produce(
                            'votes_topic',
                            key=vote["voter_id"],
                            value=json.dumps(vote),
                            on_delivery=delivery_report
                        )
                        producer.poll(0)  # Poll producer to send message

                    except Exception as e:
                        print(f"Error inserting vote into database: {e}")
                        conn.rollback()

                # Sleep to control the polling rate
                time.sleep(2)

        except KafkaException as e:
            print(f"Kafka error: {e}")
        finally:
            consumer.close()

    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Ensure proper cleanup of resources
        if cur:
            cur.close()
        if conn:
            conn.close()
