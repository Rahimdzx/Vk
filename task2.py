import psycopg2
import pika
import json

# RabbitMQ connection parameters
rabbitmq_params = pika.ConnectionParameters(
    host='localhost',
    port=5672,  # Default RabbitMQ port
    credentials=pika.PlainCredentials('guest', 'guest'),
    virtual_host='/'   # Default virtual host
)

# PostgreSQL connection parameters
dbname = 'rahimdzx'
user = 'postgres'
password = 'rahim'
host = 'localhost'
port = '5432'

# Connect to PostgreSQL
conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
print("Connected to PostgreSQL!")

# Create documents table and upsert function
def setup_database():
    commands = (
        """
        CREATE TABLE IF NOT EXISTS documents (
            url TEXT PRIMARY KEY,
            pub_date BIGINT,
            fetch_time BIGINT,
            text TEXT,
            first_fetch_time BIGINT
        )
        """,
        """
        CREATE OR REPLACE FUNCTION upsert_document(
            p_url TEXT,
            p_pub_date BIGINT,
            p_fetch_time BIGINT,
            p_text TEXT
        ) RETURNS VOID AS $$
        DECLARE
            existing_doc RECORD;
        BEGIN
            -- Check if the document exists
            SELECT * INTO existing_doc FROM documents WHERE url = p_url;

            IF existing_doc IS NOT NULL THEN
                -- Update existing document
                UPDATE documents
                SET 
                    pub_date = LEAST(existing_doc.pub_date, p_pub_date),
                    fetch_time = GREATEST(existing_doc.fetch_time, p_fetch_time),
                    text = CASE WHEN p_fetch_time > existing_doc.fetch_time THEN p_text ELSE existing_doc.text END,
                    first_fetch_time = LEAST(existing_doc.first_fetch_time, p_fetch_time)
                WHERE url = p_url;
            ELSE
                -- Insert new document
                INSERT INTO documents (url, pub_date, fetch_time, text, first_fetch_time)
                VALUES (p_url, p_pub_date, p_fetch_time, p_text, p_fetch_time);
            END IF;
        END;
        $$ LANGUAGE plpgsql;
        """
    )

    try:
        # Execute each command
        with conn.cursor() as cur:
            for command in commands:
                cur.execute(command)
            # Commit all changes
            conn.commit()
        print("Database setup completed successfully!")

    except psycopg2.Error as e:
        conn.rollback()  # Rollback in case of error
        print(f"Error setting up database: {e}")

# Function to upsert document in PostgreSQL
def upsert_document(p_url, p_pub_date, p_fetch_time, p_text):
    try:
        with conn.cursor() as cur:
            # Check if the document exists
            cur.execute("SELECT * FROM documents WHERE url = %s", (p_url,))
            existing_doc = cur.fetchone()  # Fetch one row

            if existing_doc is not None:
                # Extract values from the tuple using indexes
                existing_pub_date = existing_doc[1]  # Assuming pub_date is the second column (index 1)
                existing_fetch_time = existing_doc[2]  # Assuming fetch_time is the third column (index 2)
                existing_text = existing_doc[3]  # Assuming text is the fourth column (index 3)
                existing_first_fetch_time = existing_doc[4]  # Assuming first_fetch_time is the fifth column (index 4)

                # Update existing document
                cur.execute("""
                    UPDATE documents
                    SET 
                        pub_date = LEAST(%s, %s),
                        fetch_time = GREATEST(%s, %s),
                        text = CASE WHEN %s > fetch_time THEN %s ELSE %s END,
                        first_fetch_time = LEAST(%s, %s)
                    WHERE url = %s
                """, (existing_pub_date, p_pub_date,
                      existing_fetch_time, p_fetch_time,
                      p_fetch_time, p_text, existing_text,
                      existing_first_fetch_time, p_fetch_time,
                      p_url))
            else:
                # Insert new document
                cur.execute("""
                    INSERT INTO documents (url, pub_date, fetch_time, text, first_fetch_time)
                    VALUES (%s, %s, %s, %s, %s)
                """, (p_url, p_pub_date, p_fetch_time, p_text, p_fetch_time))
            
            conn.commit()
            print(f"Document upserted in PostgreSQL: {p_url}")

    except psycopg2.Error as e:
        conn.rollback()  # Rollback in case of error
        print(f"Error upserting document in PostgreSQL: {e}")

# Function to process incoming messages from RabbitMQ
def callback(ch, method, properties, body):
    try:
        # Decode message body from JSON
        message = json.loads(body)

        # Extract document fields
        url = message['url']
        pub_date = message['pub_date']
        fetch_time = message['fetch_time']
        text = message['text']

        # Call the upsert function to update PostgreSQL
        upsert_document(url, pub_date, fetch_time, text)

        print(f"Processed document from RabbitMQ: {url}")

        # Acknowledge message delivery
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"Error processing message: {e}")
        # Reject and requeue message
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=True)

# Main function to set up database and start consuming from RabbitMQ
def main():
    setup_database()  # Ensure the database is set up

    # Connect to RabbitMQ server and start consuming messages
    try:
        # Establish connection to RabbitMQ server
        connection = pika.BlockingConnection(rabbitmq_params)
        channel = connection.channel()

        # Declare the queue
        channel.queue_declare(queue='documents_queue')

        # Set up consumer to process messages from the queue
        channel.basic_consume(queue='documents_queue', on_message_callback=callback)

        # Start consuming (blocking operation)
        print('Waiting for messages from RabbitMQ...')
        channel.start_consuming()

    except KeyboardInterrupt:
        print("Interrupted. Closing...")

    except Exception as e:
        print(f"Error in main loop: {e}")

    finally:
        # Close PostgreSQL connection
        if conn is not None:
            conn.close()

        # Close connection to RabbitMQ
        if connection.is_open:
            connection.close()

if __name__ == "__main__":
    main()
