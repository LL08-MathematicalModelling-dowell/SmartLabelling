import kafka from '../services/kafkaService.js';
import { v4 as uuidv4 } from "uuid";
// import { sendtoKafka } from '../utils/kafkaUtil.js'
import { getSchoolInfo } from '../utils/dbUtils.js';

async function sendtoKafka(data) {
    const producer = kafka.producer();
    await producer.connect();
    await producer.send({
            topic: process.env.KAFKA_TOPIC,
            messages: [{
                "key": uuidv4(),
                // "value": JSON.stringify({"name":"Exhibitor1","scans":"scans"})
                "value": JSON.stringify(data)
            }],
               
        });

}
export async function createScannerType(req, res) {
    console.log("This is the body:", req.body)
   
    let payload = {
        schoolId: req.body.schoolId,
        scannerType: req.body.scannerType,
        dataType: "newScannerType",
    };
    console.log("This is the topic:",process.env.KAFKA_TOPIC)
    try {
            await sendtoKafka(payload);
        
        res.status(200).json({ success: true, count: payload.length});
    } catch (err) {
        console.error("❌ Failed to send scanner type to Kafka", err);
        res.status(500).json({ error: "Failed to send scanner type" });
    }
}

export async function getScannerTypes(req, res) {
    const dbName = await getSchoolInfo({schoolId: req.body.schoolId, fields: ["db_name"]});
    
    try {
       
        const results = await datacube.collectionRetrieval(dbName)
        if (results.success){
            let scannerTypes = []
            results.collections.forEach(element => {
                scannerTypes.push(element.name)
            })
            res.status(200).json({ success: true, data: scannerTypes});
        }else {
            console.error("❌ Failed to get scannerTypes 404");
            res.status(404).json({ error: "ScannerTypes not found" });
        }
        
    } catch (err) {
        console.error("❌ Failed to get scannerTypes:", err);
        res.status(500).json({ error: "Failed to get scannerTypes" });
    }
}