export async function createSchool(req, res) {
    console.log("This is the body:", req.body)
    const domainName = req.body.domainName;
    delete req.body.domainName;
    const adminId = uuidv4();
    const schoolId = uuidv4();
    const defaultURL = `${domainName}/dowellSmartLabelling/admin/?token=${adminId}`

    let school = { ...req.body, 
        schoolId: schoolId,
        urls: {
            default: defaultURL
        }
    };
    console.log(`This is the exhibitor:${exhibitor}, type:${typeof exhibitor}`)
    // exhibitor.dataType = "exhibitor";
    console.log("This is the topic:",process.env.KAFKA_TOPIC)
    try {
            await sendtoKafka(exhibitor);
        
        res.status(200).json({ success: true, count: exhibitor.length, url: url });
    } catch (err) {
        console.error("❌ Failed to send exhibitor to Kafka", err);
        res.status(500).json({ error: "Failed to send exhibitor" });
    }
}

export async function getSchools(req, res) {
    const databaseId = process.env.DATABASE_ID;
    console.log(`Database ID: ${databaseId}`);
    const collectionName = process.env.MONGO_EXHIBITOR_COLL;

    try {
        const fil = req.query.filters;
        console.log(`Filters: ${fil}`);
        const results = await datacube.dataRetrieval(databaseId, collectionName, fil)
        if (results.success){
            res.status(200).json({ success: true, data: results.data, message: "Datacube data" });
        }else {
            const mongoResults = await ExhibitorSchema.find({});
            res.status(200).json({ success: true, data: mongoResults, message: "Mongo Data"});
        }
        
    } catch (err) {
        console.error("❌ Failed to get exhibitors", err);
        res.status(500).json({ error: "Failed to get exhibitors" });
    }
}