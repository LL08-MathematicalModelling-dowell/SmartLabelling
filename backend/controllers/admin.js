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

export async function createScanner(req, res) {
    console.log("This is the body:", req.body)
   
    let payload = {
        ...req.body,
        dataType: "newScanner"
    };
    console.log("This is the topic:",process.env.KAFKA_TOPIC)
    try {
            await sendtoKafka(payload);
        
        res.status(200).json({ success: true, message: "Scanner created successfully"});
    } catch (err) {
        console.error("❌ Failed to send scanner data to Kafka", err);
        res.status(500).json({ error: "Failed to send scanner data" });
    }
}

export async function getScannerList(req, res) {
    const dbName = await getSchoolInfo({schoolId: req.body.schoolId, fields: ["db_name"]});
    console.log("This is the db name:", dbName);
    
    try {
       
        const results = await datacube.dataRetrieval(dbName.db_name, req.body.scannerType, JSON.stringify({}))
        if (results.success){
            results.data.forEach(element => {
                delete element._id
            })
            res.status(200).json({ success: true, message: "Retrieved scanner data successfully", data: results.data});
        }else {
            console.error("❌ Failed to get scanners 404");
            res.status(404).json({ error: "Scanners not found" });
        }
        
    } catch (err) {
        console.error("❌ Failed to get scanners:", err);
        res.status(500).json({ error: "Failed to get scanners" });
    }
}

export async function createStudent(req, res) {
    console.log("This is the body:", req.body)
   
    let payload = {
        ...req.body,
        dataType: "newStudent"
    };
    console.log("This is the payload:", payload)
    console.log("This is the topic:",process.env.KAFKA_TOPIC)
    try {
            await sendtoKafka(payload);
        
        res.status(200).json({ success: true, message: "Student created successfully"});
    } catch (err) {
        console.error("❌ Failed to send student data to Kafka", err);
        res.status(500).json({ error: "Failed to send student data" });
    }
}

export async function getAllStudents(req, res) {
    const filters = req.body.filters
    const dbName = await getSchoolInfo({schoolId: req.body.schoolId, fields: ["db_name"]});
    console.log("This is the db name:", dbName);
    
    try {
        const results = await datacube.dataRetrieval(dbName.db_name, req.body.scannerType, JSON.stringify(filters))
        if (results.success){
            results.data.forEach(element => {
                delete element._id
            })
            res.status(200).json({ success: true, message: "Retrieved students data successfully", data: results.data});
        }else {
            console.error("❌ Failed to get students 404");
            res.status(404).json({ error: "Students not found" });
        }
        
    } catch (err) {
        console.error("❌ Failed to get students:", err);
        res.status(500).json({ error: "Failed to get students" });
    }
}

export async function getStudent(req, res) {
    const filters = {
        studentId: req.body.studentId
    }
    const dbName = await getSchoolInfo({schoolId: req.body.schoolId, fields: ["db_name"]});
    console.log("This is the db name:", dbName);
    
    try {
        const results = await datacube.dataRetrieval(dbName.db_name, req.body.scannerType, JSON.stringify(filters))
        if (results.success){
            results.data.forEach(element => {
                delete element._id
            })
            res.status(200).json({ success: true, message: "Retrieved student data successfully", data: results.data});
        }else {
            console.error("❌ Failed to get student 404");
            res.status(404).json({ error: "Student not found" });
        }
        
    } catch (err) {
        console.error("❌ Failed to get student:", err);
        res.status(500).json({ error: "Failed to get student" });
    }
}


