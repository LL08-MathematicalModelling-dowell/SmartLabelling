import { Router } from "express";
import { createScannerType } from "../controllers/admin.js";

const router = Router();

router.post("/scanner-type", createScannerType);
// router.get("/scanner-type", createScannerType);

export default router;