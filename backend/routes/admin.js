import { Router } from "express";
import { createScannerType, getScannerTypes } from "../controllers/admin.js";

const router = Router();

router.post("/scanner-type", createScannerType);
router.get("/scanner-type", getScannerTypes);

export default router;