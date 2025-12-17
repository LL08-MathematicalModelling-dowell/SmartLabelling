import jwt from "jsonwebtoken";
import Datacubeservices from "./datacube.services.js"; 

const datacube = new Datacubeservices(process.env.DATACUBE_API_KEY);
const JWTDecode = (token) => {
    try {
    const decoded = jwt.verify(token, process.env.JWT_SECRET);
    console.log("✅ Original payload:", decoded);
    return decoded;
    } catch (error) {
    console.error("❌ Invalid or expired token:", error.message);
    }
}

function cleanString(str) {
  return str
    .trim()
    .replace(/\s+/g, '-')           // replace spaces with hyphens
    .replace(/[^a-zA-Z0-9_-]/g, ''); // remove unwanted characters
}

const insertSchoolData =  async (data) => {
    const dbName = cleanString(data.data.schoolName) + "_" + data.schoolId;
    const collections = [
        {
            name: "students",
            fields: [ 
                {"name":"studentId","type":"string"},
                {"name":"studentName","type":"string"},
                {"name":"db_name","type":"string"},
                {"name":"isActive","type":"boolean"},
                {"name":"timestamp","type":"string"}
            ]
        },
        {
            name: "teachers",
            fields: [ 
                {"name":"teacherId","type":"string"},
                {"name":"teacherName","type":"string"},
                {"name":"isActive","type":"boolean"},
                {"name":"timestamp","type":"string"}
            ]
        }
    ]
    const response = await datacube.dbCreation(dbName, collections);
    if (response.success) {
        console.log("Database created successfully in datacube:", response.message);
        const db_name = response.database.id
        data.db_name = db_name
        delete data.dataType;
        const res = await datacube.dataInsertion(process.env.MASTER_DATABASE_ID, process.env.MASTER_COLL_NAME, data);
        if (res.success) {
           console.log('Data inserted successfully in datacube:', res.message);
           return res 
        } else {
            console.error('Error inserting data:', res.error);
            return res
        }
    } else {
        console.error('Error creating database:', response.error);
        return response
    }

}
const getSchoolInfo = async (data) => {
    const response = await datacube.dataRetrieval(
        process.env.MASTER_DATABASE_ID, 
        process.env.MASTER_COLL_NAME, 
        JSON.stringify({
            "schoolId": data.schoolId
        })
    );
    if (response.success) {
        let results = {}
        data.fields.forEach(element => {
            results[element] = response.data[0][element]
        });
       
        console.log('Results: ', results);
        console.log('Data retrieved successfully from datacube:', response);
        return results;
        
    } else {
        console.error('Error retrieving school info:', response.error);
        return response
    }
}

const createScannerType = async (data) => {
    let dbData = await getSchoolInfo({schoolId: data.schoolId, fields: ["db_name"]});
    const dbName = dbData.db_name
 
    const collections = [
        {
            name: data.scannerType,
            fields: [ 
                {"name":"personId","type":"string"},
                {"name":"personName","type":"string"},
                {"name":"isActive","type":"string"},
                {"name":"timestamp","type":"string"}
            ]
        }
    ]
    console.log("This is the db name:", dbName);
    console.log(`scanner type: ${data.scannerType}`);
    const response = await datacube.createCollection(dbName, collections);
    if (response.success) {
        console.log("ScannerType coll created successfully in datacube:", response.message);
        return response
    } else {
        console.error('Error creating ScannerType coll:', response.error);
        return response
    }
}

export { JWTDecode, insertSchoolData, createScannerType };