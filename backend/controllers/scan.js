import kafka from '../services/kafkaService.js';
import Datacubeservices from '../services/datacubeServices.js';
import { v4 as uuidv4 } from "uuid";
import { getSchoolInfo } from '../utils/dbUtils.js';

const datacube = new Datacubeservices(process.env.DATACUBE_API_KEY);
async function sendtoKafka(data) {
    const producer = kafka.producer();
    await producer.connect();
    await producer.send({
            topic: process.env.KAFKA_TOPIC,
            messages: [{
                "key": uuidv4(),
                "value": JSON.stringify(data)
            }],
               
        });

}
export async function sendScans(req, res) {
    let { scans } = req.body;
    scans.dataType = "newScanData"
    console.log("This is the topic:",process.env.KAFKA_TOPIC)
    try {
            await sendtoKafka(scans);
        
        res.status(200).json({ success: true, count: scans.length });
    } catch (err) {
        console.error("❌ Failed to send scans to Kafka", err);
        res.status(500).json({ error: "Failed to send scans" });
    }
}