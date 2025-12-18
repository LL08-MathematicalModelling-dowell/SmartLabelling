import { Router } from "express";
import { createScannerType, getScannerTypes, createScanner, getScannerList, createStudent, getAllStudents, getStudent } from "../controllers/admin.js";

const router = Router();

router.post("/scanner-type", createScannerType);
router.get("/scanner-type", getScannerTypes);
router.post("/add-scanner", createScanner);
router.get("/get-scanners", getScannerList);
router.post("/student", createStudent);
router.get("/student", getStudent);
router.get("/students", getAllStudents);

export default router;