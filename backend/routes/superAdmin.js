import { Router } from "express";
import { createSchool, getSchools } from "../controllers/superAdmin.js";

const router = Router();

router.post("/schools", createSchool);
router.get("/schools", getSchools);

export default router;