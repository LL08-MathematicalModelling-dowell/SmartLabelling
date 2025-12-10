import { Router } from "express";
import {
  healthCheckService
} from "../controllers/health.js";

const router = Router();

router.get("/", healthCheckService);

export default router;