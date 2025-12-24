import { Router } from "express";
import { createSchool, getSchools } from "../controllers/superAdmin.js";
import { generateQRCode } from "../controllers/qrCode.js";

const router = Router();

router.post("/schools", createSchool);
router.get("/schools", getSchools);

router.post("/create-qr", generateQRCode)

export default router;