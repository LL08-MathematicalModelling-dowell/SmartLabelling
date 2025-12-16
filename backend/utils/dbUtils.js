import Datacubeservices from "../services/datacubeServices.js"; 

const datacube = new Datacubeservices(process.env.DATACUBE_API_KEY);
export const getSchoolInfo = async (data) => {
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
            results[element] = response.data.element
        });
        console.log('Data retrieved successfully from datacube:', response.message);
        return results;
        
    } else {
        console.error('Error retrieving school info:', response.error);
        return response
    }
}
