# feedalert_mqtt_to_postgres

This captures the MQTT stream from the main broker and stores the message into a postgres database.
This is broken down into two tables:

MQTT - A copy of the messsage, somewhat parsed.
Things - A specific record of the Thing's attributes and properties. the script will automatically alter the columns when new attributes and properties are discovered in the stream.
