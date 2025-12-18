import { Router } from "express";
import { sendScans} from "../controllers/scan.js";

const router = Router();

router.post("/scans", sendScans);

export default router;