"""
    This program listens for messages contiously and processes multiple consumers. 

    Author: Kristen Finley
    Date: Oct 4, 2023

"""

import pika
import sys
import csv

# Configure logger
from util_logger import setup_logger

# Import function to send email alerts
from emailer import createAndSendEmailAlert

logger, logname = setup_logger(__file__)

# define a callback function to be called when a message is received for type
def callback1_type(ch, method, properties, body):
    """ Define behavior on getting a message."""
    # decode the binary message body to a string
    logger.info(f" [x] Received {body.decode()}")
    # Decode the binary message body to a string
    recieved_message1 = body.decode()
    message1 = str(recieved_message1.upper())
    
    # Write new message to a file
    with open("consumer1_type.csv", "a") as output_file:
        writer = csv.writer(output_file, delimiter=",")
        writer.writerow(message1.split(','))   
        
    # when done with task, tell the user
    logger.info(f" [x] Processed {message1}.")
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)

# define a callback function to be called when a message is received for location
def callback2_location(ch, method, properties, body):
    """ Define behavior on getting a message."""
    # decode the binary message body to a string
    logger.info(f" [x] Received {body.decode()}")
    # Decode the binary message body to a string
    recieved_message2 = body.decode()
    message2 = str(recieved_message2.upper())
      
    # check for price under $1150 and send alert
    if "CITY: COLUMBIA, STATE: MISSOURI" in message2:
        print("========================================")
        print(f"ALERT {message2}")
        print("========================================")
        
        logger.info(f"ALERT! Brewery located in Columbia, MO.")
        email_subject = "ALERT! Brewery found in Columbia"
        email_body = (f"Brewery Information: {message2} ")
        createAndSendEmailAlert(email_subject, email_body)  
        

    # Write new message to a file
    with open("consumer2_location.csv", "a") as output_file:
        writer = csv.writer(output_file, delimiter=",")
        writer.writerow(message2.split(','))   
        
    # when done with task, tell the user
    logger.info(f" [x] Processed {message2}.")
    # acknowledge the message was received and processed 
    # (now it can be deleted from the queue)
    ch.basic_ack(delivery_tag=method.delivery_tag)

# define a main function to run the program
def main(hn: str = "localhost"):
    """ Continuously listen for task messages on multiple named queues."""

    # create a list of queues and their associated callback functions
    queues_and_callbacks = [
        ("01-type", callback1_type),
        ("02-location", callback2_location),
    ]

    try:
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

        # use the connection to create a communication channel
        channel = connection.channel()

        for queue_name, callback_function in queues_and_callbacks:
            # use the channel to declare a durable queue
            # a durable queue will survive a RabbitMQ server restart
            # and help ensure messages are processed in order
            # messages will not be deleted until the consumer acknowledges
            channel.queue_declare(queue=queue_name, durable=True)

            # The QoS level controls the # of messages
            # that can be in-flight (unacknowledged by the consumer)
            # at any given time.
            # Set the prefetch count to one to limit the number of messages
            # being consumed and processed concurrently.
            # This helps prevent a worker from becoming overwhelmed
            # and improve the overall system performance.
            # prefetch_count = Per consumer limit of unacknowledged messages
            channel.basic_qos(prefetch_count=1)

            # configure the channel to listen on a specific queue,
            # use the appropriate callback function,
            # and do not auto-acknowledge the message (let the callback handle it)
            channel.basic_consume(queue=queue_name, on_message_callback=callback_function, auto_ack=False)

        # log a message for the user
        logger.info(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        logger.info("")
        logger.error("ERROR: Something went wrong.")
        logger.error(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("")
        logger.info("User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        logger.info("\nClosing connection. Goodbye.\n")
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main("localhost")