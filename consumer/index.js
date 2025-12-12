import kafka from './kafka-client.js';
import 'dotenv/config';
import Datacubeservices from './datacube.services.js';
import { v4 as uuidv4 } from 'uuid';

// Environment variables from Docker Compose
const topic = process.env.KAFKA_TOPIC;
const groupId = process.env.KAFKA_GROUP_ID;
const consumer = kafka.consumer({ groupId: groupId });
const datacube = new Datacubeservices(process.env.DATACUBE_API_KEY);

/**
 * Main function to run the consumer worker.
 */
const run = async () => {
    // Connect and subscribe the Kafka consumer
    await consumer.connect();
    console.log('Kafka Consumer connected.');
    console.log("KAFKA_TOPIC;", process.env.KAFKA_TOPIC);
    await consumer.subscribe({ topic: topic, fromBeginning: false });
    console.log(`Subscribed to Kafka topic: ${topic} with group ID: ${groupId}`);

    // Start consuming messages
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            try {
                const data = JSON.parse(message.value.toString());
                console.log(`Received message from partition ${partition}:`, data);
                if (data.dataType == 'exhibitor') {
                    console.log("School has been created")
                    // const collections =[{
                    //             name: data.name+"_"+data.exhibitorId,
                    //             fields: [ 
                    //                 {"name":"data","type":"string"},
                    //                 {"name":"timestamp","type":"string"}
                    //             ]
                    //     }]
                    // const response  = await datacube.createCollection(process.env.DATABASE_ID,collections)
                    // if (response.success) {
                    //     delete data.dataType;
                    //     const res = await datacube.dataInsertion(process.env.DATABASE_ID, exhibitorCollection, data);
                    //     const tokenRes = await datacube.dataInsertion(process.env.DATABASE_ID, tokenCollectionName, tokenData);
                    //     console.log("This is the exhibitor insertion response",res);
                    //     data.datacube_success = true; 
                    //     console.log('Collection created successfully in datacube:', response.message);
                    // } else {
                    //     data.datacube_success = false;
                    //     console.error('Error creating collection:', response.error);
                    // }
                }else{
                    console.log("Inside the else statement")
                    // for (let i = 0; i < data.length; i++) {
                    //     console.log(data[i]);
                    //     delete data[i].tokenId
                    //     data[i].timestamp = new Date().toISOString();
                    //     const response = await datacube.dataInsertion(process.env.DATABASE_ID, collectionName, data[i]);
                    //     if (response.success) {
                    //         data[i].datacube_success = true; 
                    //         console.log('Scan inserted successfully in datacube:', response.message);
                    //     } else {
                    //         data[i].datacube_success = false;
                    //         console.error('Error inserting scan:', response.error);
                    //     }
                    // }
                    
                }
                const documentToInsert = {
                    ...data,
                    processedAt: new Date(),
                    kafkaMetadata: {
                        topic,
                        partition,
                        offset: message.offset.toString(), // Store offset as string for compatibility
                    }
                };

            } catch (err) {
                console.error('Error processing message:', err);
            }
        },
    });
};

run().catch(async (error) => {
    console.error('An error occurred in the consumer worker:', error);
    await shutdown();
    process.exit(1);
});

// Graceful shutdown logic
const shutdown = async () => {
    console.log('Shutting down consumer worker...');
    try {
        await consumer.disconnect();
        console.log('Kafka Consumer disconnected.');
    } catch (e) {
        console.error('Error disconnecting Kafka consumer', e);
    }
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
