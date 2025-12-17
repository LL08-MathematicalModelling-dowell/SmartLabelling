import kafka from '../services/kafkaService.js';
import Datacubeservices from '../services/datacubeServices.js';
import { v4 as uuidv4 } from "uuid";
// import { sendtoKafka } from '../utils/kafkaUtil.js'
import { getSchoolInfo } from '../utils/dbUtils.js';

const datacube = new Datacubeservices(process.env.DATACUBE_API_KEY);
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
    console.log("This is the db name:", dbName);
    
    try {
       
        const results = await datacube.collectionRetrieval(dbName.db_name)
        if (results.success){
            let scannerTypes = []
            results.collections.forEach(element => {
                scannerTypes.push(element.name)
            })
            res.status(200).json({ success: true, message: "Retrieved scanner types successfully", available_types: scannerTypes});
        }else {
            console.error("❌ Failed to get scannerTypes 404");
            res.status(404).json({ error: "ScannerTypes not found" });
        }
        
    } catch (err) {
        console.error("❌ Failed to get scannerTypes:", err);
        res.status(500).json({ error: "Failed to get scannerTypes" });
    }
}