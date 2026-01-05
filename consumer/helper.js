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
    const dbName = data.schoolId;
    console.log("Database name (insertSchoolData):", dbName, dbName.length);
    const collections = [
        { 
            name: "students",
            fields: [
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

const createScanner = async (data) => {
    let dbData = await getSchoolInfo({schoolId: data.schoolId, fields: ["db_name"]});
    const dbName = dbData.db_name
    const scannerType = data.scannerType
    delete data.scannerType
    delete data.schoolId
    const response = await datacube.dataInsertion(dbName, scannerType, data);
    if (response.success) {
        console.log('Data inserted successfully in datacube:', response.message);
        return response 
    } else {
        console.error('Error inserting data:', response.error);
        return response
    }
}

function generateDates(year, month = null) {
  const dates = [];

  // Helper to format date as DD-MM-YYYY
  function formatDate(date) {
    const dd = String(date.getDate()).padStart(2, "0");
    const mm = String(date.getMonth() + 1).padStart(2, "0");
    const yyyy = date.getFullYear();
    return `${dd}-${mm}-${yyyy}`;
  }

  // If month is provided (1–12)
  if (month !== null) {
    const startDate = new Date(year, month - 1, 1);
    const endDate = new Date(year, month, 0);

    for (let d = new Date(startDate); d <= endDate; d.setDate(d.getDate() + 1)) {
      dates.push(formatDate(d));
    }
  } 
  // If only year is provided
  else {
    const startDate = new Date(year, 0, 1);
    const endDate = new Date(year, 11, 31);

    for (let d = new Date(startDate); d <= endDate; d.setDate(d.getDate() + 1)) {
      dates.push(formatDate(d));
    }
  }

  return dates;
}

const createStudent = async (data) => {
    console.log("This is the data (helper.js):", data);
    const dbName = data.studentId + "_" + data.schoolId;
    const collections = []

    const fields = [
                {"name":"timestamp","type":"string"},
                {"name":"location","type":"string"}
            ]
    // const collectionNames = generateDates(new Date().getFullYear(), new Date().getMonth() + 1); 
    const collectionNames = generateDates(data.year);
    // console.log("This is the collection names:", collectionNames);
    
    for (let i = 0; i < collectionNames.length; i++) {
        // console.log(`Adding collection ${i} of ${collectionNames.length} to list`);
        collections.push({
            name: collectionNames[i],
            fields: fields
        })
    }
    // console.log("This is the collections:", collections);

    const response = await datacube.dbCreation(dbName, collections);
    console.log("RESPONSE FOR STUDENT DB CREATION:", response);

    if (response.success) {
        console.log("Student database created successfully in datacube:", response.message);
        const db_name = response.database.id
        data.db_name = db_name
        delete data.dataType;

        let dbData = await getSchoolInfo({schoolId: data.schoolId, fields: ["db_name"]});
        const dbID = dbData.db_name
        console.log("THIS IS THE DATA:", dbData);
        delete data.schoolId

        let collections = [
        {
            name: data.studentId,
            fields: [
                {"name":"studentName","type":"string"},
                {"name":"db_name","type":"string"},
                {"name":"qrInfo","type":"string"},
                {"name":"isActive","type":"boolean"},
                {"name":"timestamp","type":"string"}
            ]
        }
        ]
        const collResponse = await datacube.createCollection(dbID, collections);
        if (collResponse.success) {
            console.log("Student coll created successfully in datacube:", collResponse.message);
            const res = await datacube.dataInsertion(dbID, data.studentId, data);
            const studentRecordRes = await datacube.dataInsertion(dbID, "students", data);
            if (studentRecordRes.success) {
                console.log('Student data inserted successfully in datacube (student collection):', studentRecordRes.message);
            
            } else {
                console.error('Error inserting student record (student collection):', studentRecordRes.error);
              
            }
            if (res.success) {
            console.log('Student data inserted successfully in datacube:', res.message);
            return res 
            } else {
                console.error('Error inserting student data:', res.error);
                return res
            }
        } else{
            console.error('Error creating student coll:', collResponse.error);
            return collResponse
        }
        
    } else {
        console.error('Error creating student database:', response.error);
        return response
    }

}

const getStudentInfo = async (data) => {
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

const saveQRCode = async (data) => {
    // delete data.qrCode
    delete data.dataType

    const dbID = data.databaseId
    const studentId = data.studentId
    console.log(`DATABASE ID is:${dbID}, studentId is: ${studentId}`);
    const response = await datacube.dataInsertion(dbID, studentId, data);
    if (response.success) {
        console.log('Data inserted successfully in datacube:', response.message);
        return response 
    } else {
        console.error('Error inserting data:', response.error);
        return response
    }
}

const sendScans = async (data) =>{

}

export { JWTDecode, insertSchoolData, createScannerType, createScanner, createStudent, saveQRCode };