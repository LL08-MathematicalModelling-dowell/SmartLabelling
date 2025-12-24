import kafka from '../services/kafkaService.js';
import Datacubeservices from '../services/datacubeServices.js';
import { v4 as uuidv4 } from "uuid";
// import { sendtoKafka } from '../utils/kafkaUtil.js'
import { getSchoolInfo } from '../utils/dbUtils.js';
import QRCodeGenerator from '../services/qrcodeService.js';

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
export async function generateQRCode(req, res) {
  try {
    const { schoolId, databaseId, domainName } = req.body;

    const qrGenerator = new QRCodeGenerator({
      baseUrl: `${domainName}/studentFeeback/`,
      schoolId,
      databaseId,
      studentId
    });

    const dataUrl = await qrGenerator.createGeneralQR();
    const { buffer, contentType } = qrGenerator.dataUrlToBuffer(dataUrl);

    const url = qrGenerator.getUrl();

    const qrDoc = {
      schoolId: schoolId,
      studentId: studentId,
      qrCode : {
        link: url,
        image: buffer,
        contentType: contentType
      },
      dataType: "newQRCode"
    };

    sendtoKafka(qrDoc);

    res.set({
      "Content-Type": contentType,
      "Content-Disposition": `attachment; filename="school-${schoolId}-qr.png"`,
    });

    res.send(buffer);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};